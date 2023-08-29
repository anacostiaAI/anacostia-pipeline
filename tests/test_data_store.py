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


class NodeTests(unittest.TestCase):
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
    
    def test_empty_setup(self):
        data_store_node = FileStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(data_store_node)

        for i in range(10):
            data_store_node.save_data_sample(content=f"test {i+1}")

        time.sleep(0.1)
        data_store_node.event.set()
        self.tearDown_node(data_store_node)

    def test_nonempty_setup(self):
        # putting files into the feature_store folder before starting
        os.makedirs(f"{self.path}", exist_ok=True)
        for i in range(5):
            create_file(f"{self.path}/data_{i+1}.txt", f"test {i+1}")
        
        data_store_node = FileStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(data_store_node)
        
        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        #time.sleep(0.1)
        data_store_node.event.set()
        
        while data_store_node.event.is_set() is True:
            # there is a strange bug where the event is cleared before the node is done updating state
            # so we wait until the node is done updating state before continuing
            # in rare cases, this will cause the test to hang here because the node is not updating state
            print(f"{self._testMethodName} waiting for node to update state")
            time.sleep(0.1)

        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("old"))))

        data_store_node.event.set()
        self.tearDown_node(data_store_node)

    def test_many_iterations(self):
        data_store_node = FileStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(data_store_node)

        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        for i in range(5):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(5, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        data_store_node.event.set()
        data_store_node.log(f"first iteration")

        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        for i in range(4):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(4, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("old"))))

        data_store_node.event.set()
        data_store_node.log(f"second iteration")

        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(4, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("old"))))

        for i in range(3):
            data_store_node.save_data_sample(content=f"test {i+1}")

        self.assertEqual(3, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(4, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(5, len(list(data_store_node.load_data_samples("old"))))

        data_store_node.event.set()
        data_store_node.log(f"fourth iteration")

        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(3, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(9, len(list(data_store_node.load_data_samples("old"))))

        data_store_node.event.set()
        data_store_node.log(f"fifth iteration")

        self.assertEqual(0, len(list(data_store_node.load_data_samples("new"))))
        self.assertEqual(0, len(list(data_store_node.load_data_samples("current"))))
        self.assertEqual(12, len(list(data_store_node.load_data_samples("old"))))

        self.tearDown_node(data_store_node)


if __name__ == "__main__":
    unittest.main()