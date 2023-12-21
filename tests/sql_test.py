import unittest
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
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)



class MonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples)
    
    def trigger_condition(self) -> bool:
        return True
    
    def create_filename(self) -> str:
        return f"data_file{self.get_num_artifacts('all')}.txt"

class NonMonitoringDataStoreNode(FilesystemStoreNode):
    def __init__(
        self, name: str, resource_path: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, resource_path, metadata_store, init_state, max_old_samples, monitoring=False)
    
    def trigger_condition(self) -> bool:
        return True
    
    def create_filename(self) -> str:
        return f"data_file{self.get_num_artifacts('all')}.txt"

class DataPreparationNode(BaseActionNode):
    def __init__(
        self, 
        name: str, 
        data_store: MonitoringDataStoreNode,
        deposit_store: NonMonitoringDataStoreNode,
        metadata_store: BaseMetadataStoreNode = None
    ) -> None:
        super().__init__(name, predecessors=[
            data_store,
            deposit_store
        ])
        self.data_store = data_store
        self.metadata_store = metadata_store
    
    def setup(self) -> None:
        self.metadata_store.create_resource_tracker(self)
    
    def execute(self, *args, **kwargs) -> bool:
        time.sleep(5)
        return True



class TestSQLiteMetadataStore(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
    
    def setUp(self) -> None:
        self.path = f"{sqlite_tests_path}/{self._testMethodName}"
        self.data_path = f"{sqlite_tests_path}/data"
        self.deposit_path = f"{sqlite_tests_path}/deposit"
        os.makedirs(self.path)

    def test_empty_pipeline(self):
        sqlite_store = SqliteMetadataStore("sqlite_metata_store", f"sqlite:///{self.path}/test.db")
        data_store = MonitoringDataStoreNode("data_store", self.data_path, sqlite_store)
        deposit_store = NonMonitoringDataStoreNode("deposit_store", self.deposit_path, sqlite_store)
        data_prep = DataPreparationNode("data_prep", data_store, deposit_store, sqlite_store)

        pipeline = Pipeline([sqlite_store, data_store, deposit_store, data_prep], loggers=logger)
        pipeline.launch_nodes()
        
        time.sleep(2)

        for i in range(10):
            create_file(f"{self.data_path}/test_file{i}.txt", f"test file {i}")
            time.sleep(1.5)
        
        time.sleep(20)
        pipeline.terminate_nodes()

        

if __name__ == "__main__":
    unittest.main()