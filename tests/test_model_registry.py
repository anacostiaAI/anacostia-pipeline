import unittest
import logging
import sys
import os
import shutil
import random
from threading import Thread

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.model_registry import ModelRegistryNode
from anacostia_pipeline.engine.constants import Status

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

model_registry_tests_path = "./testing_artifacts/model_registry_tests"
if os.path.exists(model_registry_tests_path) is True:
    shutil.rmtree(model_registry_tests_path)

os.makedirs(model_registry_tests_path)
os.chmod(model_registry_tests_path, 0o777)

log_path = f"{model_registry_tests_path}/model_registry.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)

# Create a logger
logger = logging.getLogger(__name__)


class PyTorchModelRegistryNode(ModelRegistryNode):
    def __init__(self, name: str, path: str) -> None:
        super().__init__(name, path, "pytorch")

    def create_filename(self) -> str:
        return super().create_filename("pt")
    
    def save_model(self) -> None:
        model_name = self.create_filename()
        create_file(f"{self.model_registry_path}/{model_name}", "PyTorch model")
        self.log(f"saved model {model_name} to registry {self.model_registry_path}")


class NodeTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.feature_store_dirs = os.listdir(model_registry_tests_path)
        self.path = f"{model_registry_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)

    def start_node(self, model_registry_node: PyTorchModelRegistryNode) -> None:
        model_registry_node.set_logger(logger)
        model_registry_node.num_successors = 1
        model_registry_node.start()

    def tearDown_node(self, model_registry_node: PyTorchModelRegistryNode) -> None:
        model_registry_node.stop()
        model_registry_node.join()

    def test_empty_setup(self):
        model_registry_node = PyTorchModelRegistryNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(model_registry_node)
        
        # wait for node to finish setup
        time.sleep(0.1)
        
        for _ in range(5):
            model_registry_node.save_model()
            time.sleep(0.1)

        time.sleep(0.1)
        model_registry_node.event.set()
        self.tearDown_node(model_registry_node)


if __name__ == "__main__":
    unittest.main()