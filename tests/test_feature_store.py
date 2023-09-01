import unittest
import logging
import sys
import os
import shutil
import time

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.resource.feature_store import FeatureStoreNode

import random
from test_utils import *


# Set the seed for reproducibility
seed_value = 42
random.seed(seed_value)

feature_store_tests_path = "./testing_artifacts/feature_store_tests"
if os.path.exists(feature_store_tests_path) is True:
    shutil.rmtree(feature_store_tests_path)

os.makedirs(feature_store_tests_path)
os.chmod(feature_store_tests_path, 0o777)

# Create a logger
log_path = f"{feature_store_tests_path}/feature_store.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='w'
)
logger = logging.getLogger(__name__)


class AnacostiaFeatureStoreNode(FeatureStoreNode):
    def trigger_condition(self) -> bool:
        num_new_files = len(list(self.get_feature_vectors_filepaths("new")))
        if num_new_files >= 4:
            return True
        else:
            return False


class FeatureStoreTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
    
    def setUp(self) -> None:
        self.path = f"{feature_store_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)

    def start_node(self, feature_store_node: FeatureStoreNode) -> None:
        feature_store_node.set_logger(logger)
        feature_store_node.num_successors = 1
        feature_store_node.start()

    def tearDown_node(self, feature_store_node: FeatureStoreNode) -> None:
        feature_store_node.stop()
        feature_store_node.join()

    def test_empty_setup(self):
        feature_store_node = AnacostiaFeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)
        
        self.assertEqual(0, len(list(feature_store_node.get_feature_vectors("current"))))

        for i in range(5):
            random_number = random.randint(0, 100)
            create_numpy_file(file_path=f"{self.path}/feature_store/features_{i}", shape=(random_number, 3)) 
            time.sleep(0.1)

        time.sleep(0.1)
        self.tearDown_node(feature_store_node)

    def test_nonempty_setup(self):
        # putting files into the feature_store folder before starting
        os.makedirs(f"{self.path}/feature_store")
        for i in range(5):
            random_number = random.randint(0, 100)
            create_numpy_file(file_path=f"{self.path}/feature_store/features_{i}", shape=(random_number, 3)) 
            time.sleep(0.1)
        
        feature_store_node = AnacostiaFeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)
        
        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
        
        time.sleep(0.5)
        self.tearDown_node(feature_store_node)

    def test_get_num_feature_vectors(self):
        feature_store_node = AnacostiaFeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)

        # we have not added any feature vectors yet, so the number of current feature vectors should be 0
        self.assertEqual(0, feature_store_node.get_num_feature_vectors("old"))
        self.assertEqual(0, feature_store_node.get_num_feature_vectors("current"))
        self.assertEqual(0, feature_store_node.get_num_feature_vectors("new"))

        total_num_samples = 0
        for _ in range(10):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            total_num_samples += random_number
            time.sleep(0.1)

        time.sleep(0.5)
        self.tearDown_node(feature_store_node)

    def test_save_feature_vector(self):
        feature_store_node = AnacostiaFeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)

        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            time.sleep(0.1)

        self.tearDown_node(feature_store_node)

    def test_many_iterations(self):
        feature_store_node = AnacostiaFeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)
        
        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
        
        feature_store_node.log("Starting second iteration")

        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            time.sleep(0.1)

        feature_store_node.log("Starting third iteration")

        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            time.sleep(0.1)

        self.tearDown_node(feature_store_node)


if __name__ == "__main__":
    unittest.main()