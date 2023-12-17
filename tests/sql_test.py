from logging import Logger
import os
import time
import logging
import shutil
from typing import List, Union
import requests

from dotenv import load_dotenv
from anacostia_pipeline.metadata.sql_metadata_store import SqliteMetadataStore
from anacostia_pipeline.resources.filesystem_store import FilesystemStoreNode
from anacostia_pipeline.engine.base import BaseMetadataStoreNode, BaseResourceNode
from anacostia_pipeline.engine.pipeline import Pipeline

from utils import *



load_dotenv()
sqlite_tests_path = "./testing_artifacts/sqlite_tests"
if os.path.exists(sqlite_tests_path) is True:
    shutil.rmtree(sqlite_tests_path)

os.makedirs(sqlite_tests_path)
log_path = f"{sqlite_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)

data_path = f"{sqlite_tests_path}/data"
os.makedirs(data_path)



class MonitoringDataStoreNode(FilesystemStoreNode):
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

class DataPreparationNode(BaseActionNode):
    def __init__(
        self, 
        name: str, 
        data_store: MonitoringDataStoreNode,
        metadata_store: BaseMetadataStoreNode = None
    ) -> None:
        super().__init__(name, predecessors=[
            data_store,
        ])
        self.data_store = data_store
        self.metadata_store = metadata_store
    
    def setup(self) -> None:
        self.metadata_store.create_resource_tracker(self)
    
    def execute(self, *args, **kwargs) -> bool:
        return True


if __name__ == "__main__":
    sqlite_store = SqliteMetadataStore("sqlite_metata_store", f"sqlite:///{sqlite_tests_path}/test.db")
    data_store = MonitoringDataStoreNode("data_store", data_path, sqlite_store)
    data_prep = DataPreparationNode("data_prep", data_store, sqlite_store)

    pipeline = Pipeline([sqlite_store, data_store, data_prep], loggers=logger)
    pipeline.launch_nodes()
    #time.sleep(20)
    #pipeline.terminate_nodes()