from logging import Logger
import os
import time
import logging
import shutil
from typing import List
from dotenv import load_dotenv

from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode
from anacostia_pipeline.nodes.resources.node import BaseResourceNode

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.server import RootPipelineServer

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.sqlite.node import SqliteMetadataStoreNode

from utils import *

# Make sure that the .env file is in the same directory as this Python script
load_dotenv()



class DataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples, monitoring=True)

class EvalNode(BaseActionNode):
    def __init__(
        self, name: str, data_store: BaseResourceNode, 
        metadata_store: BaseMetadataStoreNode, loggers: Logger | List[Logger] = None
    ) -> None:
        self.metadata_store = metadata_store
        self.data_store = data_store
        super().__init__(name, [data_store], loggers)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log("Evaluating LLM on Shakespeare validation dataset", level="INFO")
        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                self.log(f"Trained on {filepath}", level="INFO")
        
        for filepath in self.data_store.list_artifacts("old"):
            self.log(f"Already trained on {filepath}", level="INFO")

        # time.sleep(2)     # simulate evaluation time, uncomment to see edges light up in the dashboard
        return True

class ModelRetrainingNode(BaseActionNode):
    def __init__(
        self, name: str, 
        data_store: DataStoreNode, metadata_store: BaseMetadataStoreNode
    ) -> None:
        self.data_store = data_store
        self.metadata_store = metadata_store
        super().__init__(name, predecessors=[data_store])
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'", level="INFO")

        self.metadata_store.log_metrics(self.name, acc=1.00)
        
        self.metadata_store.log_params(
            self.name,
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
        # time.sleep(2)     # simulate training time, uncomment to see edges light up in the dashboard
        return True



web_tests_path = "./testing_artifacts/sqlite_test"
if os.path.exists(web_tests_path) is True:
    shutil.rmtree(web_tests_path)
os.makedirs(web_tests_path)

log_path = f"{web_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


path = f"{web_tests_path}/webserver_test"
metadata_store_path = f"{path}/metadata_store"
haiku_data_store_path = f"{path}/haiku"

metadata_store = SqliteMetadataStoreNode(
    name="metadata_store", 
    uri=f"{metadata_store_path}/metadata.db"
)
haiku_data_store = DataStoreNode("haiku_data_store", haiku_data_store_path, metadata_store)
eval_node = EvalNode("eval_node", haiku_data_store, metadata_store)
retraining_node = ModelRetrainingNode("retraining_node", haiku_data_store, metadata_store)
pipeline = RootPipeline(
    nodes=[metadata_store, haiku_data_store, eval_node, retraining_node], 
    loggers=logger
)



if __name__ == "__main__":
    webserver = RootPipelineServer(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000)
    webserver.run()

    time.sleep(3)
    for i in range(10):
        create_file(f"{haiku_data_store_path}/test_file{i}.txt", f"test file {i}")
        time.sleep(1.5)
    