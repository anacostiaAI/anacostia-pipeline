import sys
import os
import shutil
import random
import logging
from typing import List
import unittest

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.data_store import DataStoreNode
from anacostia_pipeline.resource.metadata_store import MetadataStoreNode
from anacostia_pipeline.resource.model_registry import ModelRegistryNode
from anacostia_pipeline.engine.node import ActionNode, BaseNode
from anacostia_pipeline.engine.pipeline import Pipeline

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

systems_tests_path = "./testing_artifacts/level3_systems_tests"
if os.path.exists(systems_tests_path) is True:
    shutil.rmtree(systems_tests_path)

os.makedirs(systems_tests_path)
os.chmod(systems_tests_path, 0o777)

# Create a logger
log_path = f"{systems_tests_path}/level3.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)



class PrelimDataStoreNode(DataStoreNode):
    def __init__(self, name: str, path: str) -> None:
        self.prelim_path = os.path.join(path, "prelim")
        if os.path.exists(self.prelim_path) is False:
            os.makedirs(self.prelim_path)
        super().__init__(name, self.prelim_path)


class DataPreprocessingNode(ActionNode):
    def __init__(self, name: str, data_store: DataStoreNode) -> None:
        self.data_store = data_store
        super().__init__(name, "preprocess", listen_to=[data_store])
    
    def execute(self, *args, **kwargs) -> bool:
        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count += 1
                break

        for file in self.data_store.load_data_paths("current"):
            time.sleep(10)
            self.logger.info(f"Processing {file}...")
        
        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count -= 1
                if self.data_store.reference_count == 0:
                    self.data_store.event.set()
                break

        return True
    
class DataIngestionNode(ActionNode):
    def __init__(self, name: str, data_preprocessing_node: DataPreprocessingNode, data_store: PrelimDataStoreNode) -> None:
        self.data_preprocessing_node = data_preprocessing_node
        self.data_store = data_store
        super().__init__(name, "ingestion", listen_to=[data_preprocessing_node])
    
    def execute(self, *args, **kwargs) -> bool:
        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count += 1
                break

        for file in self.data_store.load_data_paths("current"):
            time.sleep(10)
            self.logger.info(f"Ingesting {file}...")
        
        while True:
            with self.data_store.reference_lock:
                self.data_store.reference_count -= 1
                if self.data_store.reference_count == 0:
                    self.data_store.event.set()
                break

        return True


class ETLTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{systems_tests_path}/{self._testMethodName}"
        self.data_store_path = f"{self.path}/data_store"
        os.makedirs(self.path)
    
    def test_empty_pipeline(self):
        prelim_store = PrelimDataStoreNode("data_store", self.data_store_path)
        preprocessing = DataPreprocessingNode("preprocessing", prelim_store)
        ingestion = DataIngestionNode("ingestion", preprocessing, prelim_store)

        pipeline = Pipeline(
            nodes=[prelim_store, preprocessing, ingestion],
            logger=logger
        )
        pipeline.start()
        
        # copy files to prelim store
        files_list = [
            "./testing_artifacts/data_store/val_splits/val_4.npz", 
            "./testing_artifacts/data_store/val_splits/val_5.npz", 
            "./testing_artifacts/data_store/val_splits/val_6.npz", 
            "./testing_artifacts/data_store/val_splits/val_7.npz", 
        ]
        for path in files_list:
            shutil.copy(
                src=path, 
                dst=prelim_store.prelim_path
            )
            time.sleep(1)
        
        # Wait for training to finish
        time.sleep(30)

        pipeline.terminate_nodes()


if __name__ == "__main__":
    unittest.main()