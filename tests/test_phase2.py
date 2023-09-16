from typing import Any
import unittest
import logging
import sys
import os
import shutil
import random
from medmnist import PathMNIST, RetinaMNIST

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.data_store import DataStoreNode
from anacostia_pipeline.resource.feature_store import FeatureStoreNode
from anacostia_pipeline.engine.node import ActionNode, ResourceNode
from anacostia_pipeline.engine.pipeline import Pipeline

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

systems_tests_path = "./testing_artifacts/phase2_system_tests"
if os.path.exists(systems_tests_path) is True:
    shutil.rmtree(systems_tests_path)

os.makedirs(systems_tests_path)
os.chmod(systems_tests_path, 0o777)

# Create a logger
log_path = f"{systems_tests_path}/phase2.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


class PathMNISTDataStoreNode(DataStoreNode):
    def __init__(self, name: str, split: str, path: str, data_path: str) -> None:
        self.test_dir = os.path.join(path, "test")
        if os.path.exists(self.test_dir) is False:
            os.makedirs(self.test_dir)
        
        self.path_mnist = PathMNIST(split=split, root=data_path)
        super().__init__(name, self.test_dir, init_state="old", max_old_samples=None)

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def __getitem__(self, index):
        return self.path_mnist.__getitem__(index)


class RetrainingTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{systems_tests_path}/{self._testMethodName}"
        self.data_path = "./testing_artifacts"
        self.data_store_path = f"{self.path}/data_store"
        os.makedirs(self.path)
    
    def test_initial_setup(self):
        test_store = PathMNISTDataStoreNode(name="PathMNIST test store", split="test", path=self.path, data_path=self.data_path)
        pipeline_phase2 = Pipeline(nodes=[test_store], logger=logger)
        pipeline_phase2.start()

        time.sleep(5)
        for i, (img, label) in enumerate(test_store):
            print(i, img, label)
            if i == 5:
                break
        time.sleep(5)

        pipeline_phase2.terminate_nodes()


if __name__ == "__main__":
    unittest.main()