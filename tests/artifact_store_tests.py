from typing import List
import unittest
import logging
import sys
import os
import shutil
import random
import time

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resources.artifact_store import ArtifactStoreNode
from anacostia_pipeline.engine.base import BaseActionNode
from anacostia_pipeline.engine.logic import AndAndNode, AndOrNode, OrAndNode
from anacostia_pipeline.engine.pipeline import Pipeline

from utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

artifact_store_tests_path = "./testing_artifacts/artifact_store_tests"
if os.path.exists(artifact_store_tests_path) is True:
    shutil.rmtree(artifact_store_tests_path)

os.makedirs(artifact_store_tests_path)
os.chmod(artifact_store_tests_path, 0o777)

log_path = f"{artifact_store_tests_path}/artifact_store.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)

# Create a logger
logger = logging.getLogger(__name__)


class DataStoreNode(ArtifactStoreNode):
    def __init__(
        self, name: str, path: str, tracker_filename: str, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, path, tracker_filename, init_state, max_old_samples)
    
    def check_resource(self) -> bool:
        num_new = self.get_num_artifacts("new")
        #self.log(f"Number of new artifacts: {num_new}")
        return num_new >= 2
    
    def after_update(self) -> None:
        self.log(f"{self.name} logged {self.get_num_artifacts('all')} artifacts on blockchain.")
    
    def create_filename(self) -> str:
        return f"data_file{self.get_num_artifacts('all')}.txt"

    def save_artifact(self, content: str) -> None:
        filename = self.create_filename()
        filepath = os.path.join(self.path, filename)

        # note: record_artifact should be called before create_file so that the Observer can see the file is already logged and ignore it
        self.record_artifact(filepath, "current")
        create_file(filepath, content)
        self.log(f"Saved {filename} to {self.path}")
    

class ProcessedDataStoreNode(DataStoreNode):
    def __init__(
        self, name: str, path: str, tracker_filename: str, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, path, tracker_filename, init_state, max_old_samples)
    
    def check_resource(self) -> bool:
        return self.get_num_artifacts("new") >= 2

    def create_filename(self) -> str:
        return f"processed_data_file{self.get_num_artifacts('all')}.txt"


class ModelRegistryNode(ArtifactStoreNode):
    def __init__(self, name: str, path: str, init_state: str = "new", max_old_samples: int = None) -> None:
        super().__init__(name, path, init_state, "model_registry.json", max_old_samples)

    def check_resource(self) -> bool:
        return self.get_num_artifacts("new") > 0
    

class DataPreparationNode(BaseActionNode):
    def __init__(self, name: str, data_store: DataStoreNode, processed_data_store: ProcessedDataStoreNode) -> None:
        super().__init__(name, predecessors=[data_store])
        self.data_store = data_store
        self.processed_data_store = processed_data_store
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")
        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                content = f"processed {filepath}"
                #self.processed_data_store.save_artifact(content)
                self.log(f"Processed {filepath}")
        self.log(f"Node '{self.name}' executed successfully.")
        return True


class ModelRetrainingNode(BaseActionNode):
    def __init__(self, name: str, data_store: DataStoreNode, model_registry: ArtifactStoreNode) -> None:
        self.data_store_path = data_store.path
        self.model_registry_path = model_registry.uri
        super().__init__(name, predecessors=[data_store])
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")
        time.sleep(5)
        self.log(f"Node '{self.name}' executed successfully.")
        return True


class TestArtifactStore(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
    
    def setUp(self) -> None:
        self.path = f"{artifact_store_tests_path}/{self._testMethodName}"
        self.collection_data_store_path = f"{self.path}/collection_data_store"
        self.processed_data_store_path = f"{self.path}/processed_data_store"
        self.model_registry_path = f"{self.path}/model_registry"
        os.makedirs(self.path)
    
    def test_empty_pipeline(self):
        processed_data_store = ProcessedDataStoreNode("processed_data_store", self.processed_data_store_path, "processed_data_store.json")
        collection_data_store = DataStoreNode("collection_data_store", self.collection_data_store_path, "collection_data_store.json")
        data_prep = DataPreparationNode("data_prep", collection_data_store, processed_data_store)
        #orand = OrAndNode("or_and", [collection_data_store, processed_data_store])
        #andand = AndAndNode("andand", [collection_data_store, processed_data_store, data_prep])
        #retraining = ModelRetrainingNode("retraining", data_store)
        pipeline = Pipeline(
            nodes=[collection_data_store, processed_data_store, data_prep], 
            anacostia_path=self.path, 
            logger=logger
        )

        pipeline.launch_nodes()
        time.sleep(2)

        for i in range(10):
            create_file(f"{self.collection_data_store_path}/test_file{i}.txt", f"test file {i}")
            time.sleep(1)

        time.sleep(10)
        pipeline.terminate_nodes()

    def test_nonempty_pipeline(self):
        data_store = DataStoreNode("data_store", self.collection_data_store_path)
        data_prep = DataPreparationNode("data_prep", data_store)
        pipeline = Pipeline(nodes=[data_store, data_prep], anacostia_path=self.path, logger=logger)

        for i in range(5):
            create_file(f"{self.collection_data_store_path}/test_file{i}.txt", f"test file {i}")

        time.sleep(2)

        pipeline.launch_nodes()
        
        for i in range(5, 10):
            create_file(f"{self.collection_data_store_path}/test_file{i}.txt", f"test file {i}")
            time.sleep(1)

        time.sleep(15)
        pipeline.terminate_nodes()


if __name__ == "__main__":
    unittest.main()