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
from anacostia_pipeline.pipelines.root.app import RootPipelineApp

from anacostia_pipeline.nodes.resources.filesystem.node import FilesystemStoreNode
from anacostia_pipeline.nodes.metadata.sqlite_store.node import SqliteMetadataStoreNode

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
pipeline = RootPipeline(
    nodes=[metadata_store, haiku_data_store, eval_node], 
    loggers=logger
)



if __name__ == "__main__":
    webserver = RootPipelineApp(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000)
    webserver.run()

    time.sleep(3)
    for i in range(10):
        create_file(f"{haiku_data_store_path}/test_file{i}.txt", f"test file {i}")
        time.sleep(1.5)
    