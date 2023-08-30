from typing import Any
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

# Create a logger
log_path = f"{data_store_tests_path}/data_store.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


class FileStoreNode(DataStoreNode):
    def __init__(self, name: str, path: str) -> None:
        super().__init__(name, path)
    
    def save_data_sample(self, content: str) -> None:
        filename = self.create_filename("txt")
        create_file(f"{self.data_store_path}/{filename}", content)
        self.log(f"saved data sample {filename} to data store {self.data_store_path}")

    def load_data_sample(self, filepath: str) -> Any:
        with open(filepath, "r") as file:
            return file.read()


class DataStoreTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.path = f"{data_store_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)

    def start_node(self, data_store_node: FileStoreNode) -> None:
        data_store_node.set_logger(logger)
        data_store_node.num_successors = 1
        data_store_node.start()

    def tearDown_node(self, data_store_node: FileStoreNode) -> None:
        data_store_node.stop()
        data_store_node.join()
    
    def test_nonempty_setup(self):
        # putting files into the feature_store folder before starting
        os.makedirs(f"{self.path}", exist_ok=True)
        logger.info(f"created folder {self.path}")
        for i in range(5):
            create_file(f"{self.path}/data_{i+1}.txt", f"test {i+1}")
            logger.info(f"created file {self.path}/data_{i+1}.txt")
        
        data_store_node = FileStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(data_store_node)
        
        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        for i in range(4):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(4, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        for i in range(3):
            data_store_node.save_data_sample(content=f"test {i+1}")
        
        self.assertEqual(2, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("old"))))
        
        self.tearDown_node(data_store_node)

    def test_many_iterations(self):
        data_store_node = FileStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(data_store_node)

        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        for i in range(6):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(1, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        for i in range(6):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(2, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("old"))))

        for i in range(6):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(3, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(10, len(list(data_store_node.load_data_samples("old"))))

        for i in range(8):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(1, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(20, len(list(data_store_node.load_data_samples("old"))))

        self.tearDown_node(data_store_node)


if __name__ == "__main__":
    unittest.main()