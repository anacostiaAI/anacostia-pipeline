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
    
    # later on, this will be replaced with a function that saves a PyTorch model
    # the content argument will have a type hint of torch.nn.Module
    def save_model(self, content) -> None:

        # why didn't self.create_filename() cause a deadlock when we used a regular lock?
        model_name = self.create_filename("pt")
        create_file(f"{self.model_registry_path}/{model_name}", content)
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
        
        for _ in range(5):
            model_registry_node.save_model("PyTorch model")

        time.sleep(0.1)
        model_registry_node.event.set()
        self.tearDown_node(model_registry_node)

    def test_many_iterations(self):
        model_registry_node = PyTorchModelRegistryNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(model_registry_node)

        for _ in range(5):
            model_registry_node.save_model("PyTorch model")

        time.sleep(0.1)
        model_registry_node.event.set()
        model_registry_node.log(f"second iteration")

        for _ in range(5):
            model_registry_node.save_model("PyTorch model")
        
        time.sleep(0.1)
        model_registry_node.event.set()
        model_registry_node.log(f"third iteration")

        for _ in range(5):
            model_registry_node.save_model("PyTorch model")
        
        time.sleep(0.1)
        model_registry_node.event.set()
        self.tearDown_node(model_registry_node)

if __name__ == "__main__":
    unittest.main()