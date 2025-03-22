import os
import logging
from dotenv import load_dotenv
import argparse
from typing import List

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.sqlite.node import SqliteMetadataStoreNode
from anacostia_pipeline.pipelines.root.pipeline1 import RootPipeline
from anacostia_pipeline.pipelines.root.app1 import RootPipelineApp

from utils import *

# Make sure that the .env file is in the same directory as this Python script
load_dotenv()



parser = argparse.ArgumentParser()
parser.add_argument('root_host', type=str)
parser.add_argument('root_port', type=int)
parser.add_argument('leaf_host', type=str)
parser.add_argument('leaf_port', type=int)
args = parser.parse_args()

root_test_path = "./testing_artifacts"

log_path = f"{root_test_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='ROOT %(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)

class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    

class ModelRegistryNode(FilesystemStoreNode):
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
        self.log(f"Saved preprocessed {filepath}", level="INFO")


class PlotsStoreNode(FilesystemStoreNode):
    def __init__(self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    

class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, 
        data_store: MonitoringDataStoreNode, plots_store: PlotsStoreNode,
        model_registry: ModelRegistryNode, metadata_store: BaseMetadataStoreNode, 
        remote_successors: List[str] = None
    ) -> None:
        self.data_store = data_store
        self.model_registry = model_registry
        self.plots_store = plots_store
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_store, plots_store, model_registry], remote_successors=remote_successors)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'", level="INFO")

        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                self.log(f"Trained on {filepath}", level="INFO")
        
        for filepath in self.data_store.list_artifacts("old"):
            self.log(f"Already trained on {filepath}", level="INFO")
        
        # must pass self to log_metrics and log_params in order for the metadata store to know which node is logging
        # if you don't pass self, the metadata store will not know which node is logging and will not enter the data into the database
        self.metadata_store.log_metrics(self, acc=1.00)
        
        self.metadata_store.log_params(
            self,
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

        self.metadata_store.set_tags(self, test_name="Karpathy LLM test")

        self.log(f"Node '{self.name}' executed successfully.", level="INFO")
        return True

path = f"./root-artifacts"
input_path = f"{path}/input_artifacts"
output_path = f"{path}/output_artifacts"
metadata_store_path = f"{input_path}/metadata_store"
haiku_data_store_path = f"{input_path}/haiku"
model_registry_path = f"{output_path}/model_registry"
plots_path = f"{output_path}/plots"

metadata_store = SqliteMetadataStoreNode(
    name="metadata_store", 
    uri=f"{metadata_store_path}/metadata.db"
)
model_registry = ModelRegistryNode(
    "model_registry", 
    model_registry_path, 
    metadata_store
)
plots_store = PlotsStoreNode("plots_store", plots_path, metadata_store)
haiku_data_store = MonitoringDataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
retraining = ModelRetrainingNode(
    name="retraining", 
    data_store=haiku_data_store, 
    plots_store=plots_store, 
    model_registry=model_registry, 
    metadata_store=metadata_store, 
    remote_successors=[f"http://{args.leaf_host}:{args.leaf_port}/shakespeare_eval", f"http://{args.leaf_host}:{args.leaf_port}/haiku_eval"]
)

pipeline = RootPipeline(
    nodes=[metadata_store, haiku_data_store, model_registry, plots_store, retraining], 
    loggers=logger
)

service = RootPipelineApp(name="root", pipeline=pipeline, host=args.root_host, port=args.root_port, logger=logger)
service.run()
