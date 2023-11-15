from logging import Logger
from typing import List
import unittest
import logging
import sys
import os
import shutil
import random
import time
import traceback

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resources.artifact_store import ArtifactStoreNode
from anacostia_pipeline.resources.metadata_store import JsonMetadataStoreNode
from anacostia_pipeline.engine.base import BaseActionNode, BaseMetadataStoreNode
from anacostia_pipeline.engine.pipeline import Pipeline
from anacostia_pipeline.engine.base import Result

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


class MonitoringDataStoreNode(ArtifactStoreNode):
    def __init__(
        self, name: str, path: str, tracker_filename: str, metadata_store: BaseMetadataStoreNode, 
        init_state: str = "new", max_old_samples: int = None
    ) -> None:
        super().__init__(name, path, tracker_filename, metadata_store, init_state, max_old_samples)
    
    def trigger_condition(self) -> bool:
        num_new = self.get_num_artifacts("new")
        return num_new >= 2
    
    def create_filename(self) -> str:
        return f"data_file{self.get_num_artifacts('all')}.txt"


class NonMonitoringDataStoreNode(ArtifactStoreNode):
    def __init__(self, name: str, path: str, tracker_filename: str, metadata_store: BaseMetadataStoreNode, ) -> None:
        super().__init__(name, path, tracker_filename, metadata_store, init_state="new", max_old_samples=None, monitoring=False)
    
    def create_filename(self) -> str:
        return f"processed_data_file{self.get_num_artifacts('all')}.txt"

    def save_artifact(self, content: str) -> None:
        filename = self.create_filename()
        filepath = os.path.join(self.path, filename)

        # note: record_artifact should be called before create_file so that the Observer can see the file is already logged and ignore it
        self.record_current(filepath)
        create_file(filepath, content)
        self.log(f"Saved preprocessed {filepath}")
    
    def run(self) -> None:
        # signal to metadata store node that the resource is ready to be used; i.e., the resource is ready to be used for the next run
        # (note: change status to Work.READY)
        self.trap_interrupts()
        self.signal_predecessors(Result.SUCCESS)

        while True:
            # wait for metadata store node to finish creating the run before moving files to current
            self.trap_interrupts()
            while self.check_predecessors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)

            self.log(f"'{self.name}' is signalling successors.")

            # signalling to all successors that the resource is ready to be used
            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the current state before updating the state of the resource
            # (note: change status to Work.WAITING_SUCCESSORS)
            self.trap_interrupts()
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            
            self.log(f"'{self.name}' is updating state from current->old.")

            # moving 'current' files to 'old'
            self.trap_interrupts()
            self.current_to_old()

            # signal the metadata store node that the resource has been used for the current run
            while self.check_predecessors_signals() is False:
                self.trap_interrupts()
                self.signal_predecessors(Result.SUCCESS)
                time.sleep(0.2)

class ModelRegistryNode(ArtifactStoreNode):
    def __init__(self, name: str, path: str, init_state: str = "new", max_old_samples: int = None) -> None:
        super().__init__(name, path, init_state, "model_registry.json", max_old_samples)

    def trigger_condition(self) -> bool:
        return self.get_num_artifacts("new") > 0
    

class DataPreparationNode(BaseActionNode):
    def __init__(self, name: str, data_store: MonitoringDataStoreNode, processed_data_store: NonMonitoringDataStoreNode) -> None:
        super().__init__(name, predecessors=[data_store, processed_data_store])
        self.data_store = data_store
        self.processed_data_store = processed_data_store
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")
        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                content = f"processed {filepath}"
                self.processed_data_store.save_artifact(content)
        self.log(f"Node '{self.name}' executed successfully.")
        return True


class ModelRetrainingNode(BaseActionNode):
    def __init__(self, name: str, data_prep: DataPreparationNode, data_store: MonitoringDataStoreNode) -> None:
        self.data_store = data_store
        super().__init__(name, predecessors=[data_prep])
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing node '{self.name}'")
        for filepath in self.data_store.list_artifacts("current"):
            with open(filepath, 'r') as f:
                self.log(f"Trained on {filepath}")
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
        metadata_store = JsonMetadataStoreNode("metadata_store", "metadata_store.json")
        processed_data_store = NonMonitoringDataStoreNode(
            "processed_data_store", self.processed_data_store_path, "processed_data_store.json", metadata_store
        )
        collection_data_store = MonitoringDataStoreNode(
            "collection_data_store", self.collection_data_store_path, "collection_data_store.json", metadata_store
        )
        data_prep = DataPreparationNode("data_prep", collection_data_store, processed_data_store)
        retraining = ModelRetrainingNode("retraining", data_prep, collection_data_store)
        pipeline = Pipeline(
            nodes=[metadata_store, collection_data_store, processed_data_store, data_prep, retraining], 
            anacostia_path=self.path, 
            logger=logger
        )

        pipeline.launch_nodes()
        time.sleep(2)

        for i in range(10):
            create_file(f"{self.collection_data_store_path}/test_file{i}.txt", f"test file {i}")
            time.sleep(1)

        time.sleep(20)
        pipeline.terminate_nodes()

    """
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
    """


if __name__ == "__main__":
    unittest.main()