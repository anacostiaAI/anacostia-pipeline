import unittest
import logging
import sys
import os
import shutil
import random
from threading import Thread

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.data_store import DataStoreNode
from anacostia_pipeline.engine.constants import Status

from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

data_store_tests_path = "./testing_artifacts/data_store_tests"
if os.path.exists(data_store_tests_path) is True:
    shutil.rmtree(data_store_tests_path)

os.makedirs(data_store_tests_path)
os.chmod(data_store_tests_path, 0o777)

log_path = f"{data_store_tests_path}/data_store.log"
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
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{data_store_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)

    def start_node(self, data_store_node: DataStoreNode) -> None:
        data_store_node.set_logger(logger)
        data_store_node.num_successors = 1
        data_store_node.start()

    def tearDown_node(self, data_store_node: DataStoreNode) -> None:
        data_store_node.stop()
        data_store_node.join()
    
    def test_empty_setup(self):
        data_store_node = DataStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(data_store_node)

        for i in range(10):
            create_file(f"{self.path}/test_{i}.txt", "test")

        time.sleep(0.1)
        data_store_node.event.set()
        self.tearDown_node(data_store_node)

if __name__ == "__main__":
    unittest.main()