import unittest
import logging
import sys
import os
import shutil
import time
from threading import Thread

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.model_registry import ModelRegistryNode
from anacostia_pipeline.engine.constants import Status

from test_utils import *


log_path = "./testing_artifacts/app.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)

# Create a logger
logger = logging.getLogger(__name__)


class NodeTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        try:
            os.makedirs("./testing_artifacts")
            os.chmod("./testing_artifacts", 0o777)
        except OSError as e:
            print(f"Error occurred: {e}")

        self.model_registry_node = ModelRegistryNode(name="model_registry", path="./testing_artifacts")
        self.model_registry_node.set_logger(logger)
        self.model_registry_node.set_status(Status.RUNNING)
        self.model_registry_node.setup()

        self.thread = Thread(target=self.model_registry_node.run)
        self.thread.start()
    
        super().__init__(methodName)

    def test_setup(self):
        self.assertTrue(os.path.exists("./testing_artifacts/model_registry"))
        self.assertTrue(os.path.exists("./testing_artifacts/model_registry/old_models"))
        self.assertTrue(os.path.exists("./testing_artifacts/model_registry/new_models"))

    def tearDown(self) -> None:
        self.model_registry_node.set_status(Status.STOPPING)
        self.thread.join()
        self.model_registry_node.teardown()

        try:
            shutil.rmtree("./testing_artifacts/model_registry")
        except OSError as e:
            print(f"Error occurred: {e}")

if __name__ == "__main__":
    unittest.main()