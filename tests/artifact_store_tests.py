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
    def __init__(self, name: str, path: str, init_state: str = "current", max_old_samples: int = None) -> None:
        super().__init__(name, path, init_state, max_old_samples)


class TestArtifactStore(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
    
    def setUp(self) -> None:
        self.path = f"{artifact_store_tests_path}/{self._testMethodName}"
        self.artifact_store_path = f"{self.path}/artifact_store"
        os.makedirs(self.path)
    
    def test_empty_pipeline(self):
        data_store = DataStoreNode("data_store", self.artifact_store_path)
        pipeline = Pipeline([data_store], logger)

        pipeline.launch_nodes()
        time.sleep(2)

        for i in range(5):
            create_file(f"{self.artifact_store_path}/test_file{i}.txt", f"test file {i}")

        time.sleep(2)
        pipeline.terminate_nodes()

if __name__ == "__main__":
    unittest.main()