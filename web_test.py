import os
import time
import logging
import shutil

from anacostia_pipeline.engine.base import BaseNode, BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.web import Webserver

from anacostia_pipeline.resources.artifact_store import ArtifactStoreNode
from anacostia_pipeline.resources.metadata_store import JsonMetadataStoreNode

# n1=BaseMetadataStoreNode("test_node1")
# n2=BaseNode("test_node2", predecessors=[n1])
# pipeline = Pipeline([n1, n2])

def run_computational_task(node: BaseNode, duration_seconds: int):
    node.log(f"Node {node.name} is starting a computationally intensive task.")

    # Record the start time
    start_time = time.time()
    result = 0
    while time.time() - start_time < duration_seconds:
        for i in range(10**7):
            result += i

    node.log(f"Node {node.name} completed the computationally intensive task.")

class MonitoringDataStoreNode(ArtifactStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def trigger_condition(self) -> bool:
        num_new = self.get_num_artifacts("new")
        return num_new >= 2
    
    def create_filename(self) -> str:
        return f"data_file{self.get_num_artifacts('all')}.txt"


class NonMonitoringDataStoreNode(ArtifactStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    
    def create_filename(self) -> str:
        return f"processed_data_file{self.get_num_artifacts('all')}.txt"

    def save_artifact(self, content: str) -> None:
        filename = self.create_filename()
        filepath = os.path.join(self.path, filename)

        # note: for monitoring-enabled resource nodes, record_artifact should be called before create_file;
        # that way, the Observer can see the file is already logged and ignore it
        self.record_current(filepath)
        with open(filepath, 'w') as f:
            f.write(content)
        self.log(f"Saved preprocessed {filepath}")

class DataPreparationNode(BaseActionNode):
    def __init__(
        self, 
        name: str, 
        data_store: MonitoringDataStoreNode,
        processed_data_store: NonMonitoringDataStoreNode 
    ) -> None:
        super().__init__(name, predecessors=[
            data_store,
            processed_data_store
        ])
        self.data_store = data_store
        self.processed_data_store = processed_data_store
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")
        run_computational_task(node=self, duration_seconds=2)

        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                content = f"processed {filepath}"
                self.processed_data_store.save_artifact(content)
        self.log(f"Node '{self.name}' executed successfully.")
        return True


class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, training_duration: int, data_prep: DataPreparationNode, 
        data_store: MonitoringDataStoreNode, metadata_store: BaseMetadataStoreNode
    ) -> None:
        self.data_store = data_store
        self.training_duration = training_duration
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_prep])
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")
        run_computational_task(node=self, duration_seconds=self.training_duration)

        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                self.log(f"Trained on {filepath}")
        
        self.metadata_store.log_metrics(acc="1.00")
        
        self.metadata_store.log_params(
            batch_size = 64, # how many independent sequences will we process in parallel?
            block_size = 256, # what is the maximum context length for predictions?
            max_iters = 2500,
            eval_interval = 500,
            learning_rate = 3e-4,
            eval_iters = 200,
            n_embd = 384,
            n_head = 6,
            n_layer = 6,
            dropout = 0.2,
            seed = 1337,
            split = 0.9    # first 90% will be train, rest val
        )

        self.metadata_store.set_tags(test_name="artifact store test")

        self.log(f"Node '{self.name}' executed successfully.")
        return True

artifact_store_tests_path = "./testing_artifacts/artifact_store_tests"
if os.path.exists(artifact_store_tests_path) is True:
    shutil.rmtree(artifact_store_tests_path)

os.makedirs(artifact_store_tests_path)
log_path = f"{artifact_store_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


path = f"{artifact_store_tests_path}/webserver_test"
metadata_store_path = f"{path}/metadata_store"
collection_data_store_path = f"{path}/collection_data_store"
processed_data_store_path = f"{path}/processed_data_store"
model_registry_path = f"{path}/model_registry"

metadata_store = JsonMetadataStoreNode(
    name="metadata_store", 
    tracker_dir=metadata_store_path
)

processed_data_store = NonMonitoringDataStoreNode(
    "processed_data_store", 
    processed_data_store_path, 
    metadata_store
)
collection_data_store = MonitoringDataStoreNode("collection_data_store", collection_data_store_path, metadata_store)
data_prep = DataPreparationNode("data_prep", collection_data_store, processed_data_store)
retraining_1 = ModelRetrainingNode("retraining 1", 2, data_prep, collection_data_store, metadata_store)
retraining_2 = ModelRetrainingNode("retraining 2", 2.5, data_prep, collection_data_store, metadata_store)
pipeline = Pipeline(
    nodes=[metadata_store, collection_data_store, processed_data_store, data_prep, retraining_1, retraining_2], 
    loggers=logger
)

w = Webserver(pipeline)
w.run()

print('launching nodes')
pipeline.launch_nodes()
time.sleep(2)

for i in range(10):
    with open(f"{collection_data_store_path}/test_file{i}.txt", 'w') as f:
        f.write(f"test file {i}")
     
    time.sleep(1)

time.sleep(25)
pipeline.terminate_nodes()
print('pipeline terminated')

