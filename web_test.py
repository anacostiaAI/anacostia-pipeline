from logging import Logger
import os
import time
import logging
import shutil
from typing import List, Union
import requests
from dotenv import load_dotenv

from anacostia_pipeline.engine.base import BaseNode, BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.web import Webserver

from anacostia_pipeline.resources.artifact_store import ArtifactStoreNode
from anacostia_pipeline.resources.metadata_store import JsonMetadataStoreNode


# Make sure that the .env file is in the same directory as this Python script
load_dotenv()

"""
def run_computational_task(node: BaseNode, duration_seconds: int):
    node.log(f"Node {node.name} is starting a computationally intensive task.")

    # Record the start time
    start_time = time.time()
    result = 0
    while time.time() - start_time < duration_seconds:
        for i in range(10**7):
            result += i

    node.log(f"Node {node.name} completed the computationally intensive task.")
"""

class MonitoringDataStoreNode(ArtifactStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def trigger_condition(self) -> bool:
        num_new = self.get_num_artifacts("new")
        return num_new >= 1

class ModelRegistryNode(ArtifactStoreNode):
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


class PlotsStoreNode(ArtifactStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    

class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, 
        data_store: MonitoringDataStoreNode, plots_store: PlotsStoreNode,
        model_registry: ModelRegistryNode, metadata_store: BaseMetadataStoreNode
    ) -> None:
        self.data_store = data_store
        self.model_registry = model_registry
        self.plots_store = plots_store
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_store, plots_store, model_registry])
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")

        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                self.log(f"Trained on {filepath}")
        
        self.metadata_store.log_metrics(acc=1.00)
        
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

        self.metadata_store.set_tags(test_name="Karpathy LLM test")

        self.log(f"Node '{self.name}' executed successfully.")
        return True


class ShakespeareEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset")
        self.metadata_store.log_metrics(shakespeare_test_loss=1.47)
        return True

class HaikuEvalNode(BaseActionNode):
    def __init__(
        self, name: str, predecessors: List[BaseNode], 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Haiku validation dataset")
        self.metadata_store.log_metrics(haiku_test_loss=2.43)
        return True

class BlockchainNode(BaseActionNode):
    def __init__(self, name: str, metadata_store: JsonMetadataStoreNode, 
        predecessors: List[BaseNode], loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        super().__init__(name, predecessors, loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        """
        logic to upload to IPFS
        """

        url = "https://api.quicknode.com/ipfs/rest/v1/s3/put-object"

        tracker_dir = self.metadata_store.tracker_dir
        files_paths = [os.path.join(tracker_dir, json_file_path) for json_file_path in os.listdir(tracker_dir)]

        def send_file(path: str):
            payload = {
                'Key': path,
                'ContentType': 'text'
            }
            files=[
                ('Body', (path, open(path,'rb'),'text/json'))
            ]
            headers = {
                'x-api-key': os.getenv("API_KEY")
            }
            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            self.log(response.text)
        
        for path in files_paths:
            send_file(path)

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
haiku_data_store_path = f"{path}/haiku"
model_registry_path = f"{path}/model_registry"
plots_path = f"{path}/plots"

metadata_store = JsonMetadataStoreNode(
    name="metadata_store", 
    tracker_dir=metadata_store_path
)
model_registry = ModelRegistryNode(
    "model_registry", 
    model_registry_path, 
    metadata_store
)
plots_store = PlotsStoreNode("plots_store", plots_path, metadata_store)
haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
retraining = ModelRetrainingNode("retraining 1", haiku_data_store, plots_store, model_registry, metadata_store)
shakespeare_eval = ShakespeareEvalNode("shakespeare_eval", predecessors=[retraining], metadata_store=metadata_store)
haiku_eval = HaikuEvalNode("haiku_eval", predecessors=[retraining], metadata_store=metadata_store)
blockchain = BlockchainNode("blockchain", metadata_store, predecessors=[shakespeare_eval, haiku_eval])
pipeline = Pipeline(
    nodes=[metadata_store, haiku_data_store, model_registry, plots_store, shakespeare_eval, haiku_eval, retraining, blockchain], 
    loggers=logger
)

w = Webserver(pipeline)
w.run()

print('launching nodes')
pipeline.launch_nodes()
time.sleep(2)

haiku_partitions_dir = "./testing_artifacts/haiku"
max_files = 20
for i, filename in enumerate(os.listdir(haiku_partitions_dir)):
    if i < max_files:
        if filename not in ["haiku.csv", "testing.txt"]:
            shutil.copy(
                src=os.path.join(haiku_partitions_dir, filename),
                dst=haiku_data_store_path
            )
            time.sleep(1)

time.sleep(35)
pipeline.terminate_nodes()
print('pipeline terminated')
