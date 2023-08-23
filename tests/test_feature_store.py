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


class NodeTests(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
    
    def setUp(self) -> None:
        self.feature_store_dirs = os.listdir(feature_store_tests_path)
        self.path = f"{feature_store_tests_path}/{self._testMethodName}"
        os.makedirs(self.path)

    def start_node(self, feature_store_node: FeatureStoreNode) -> None:
        feature_store_node.set_logger(logger)
        feature_store_node.num_successors = 1
        feature_store_node.start()

    def tearDown_node(self, feature_store_node: FeatureStoreNode) -> None:
        time.sleep(0.5)
        feature_store_node.event.set()
        feature_store_node.stop()
        feature_store_node.join()

    def test_empty_setup(self):
        feature_store_node = FeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)
        
        self.assertEqual(0, len(list(feature_store_node.get_current_feature_vectors())))

        for i in range(5):
            random_number = random.randint(0, 100)
            create_numpy_file(file_path=f"{self.path}/feature_store/features_{i}", shape=(random_number, 3)) 
            time.sleep(0.1)

        self.tearDown_node(feature_store_node)

    def test_nonempty_setup(self):
        # putting files into the feature_store folder before starting
        os.makedirs(f"{self.path}/feature_store")
        for i in range(5):
            random_number = random.randint(0, 100)
            create_numpy_file(file_path=f"{self.path}/feature_store/features_{i}", shape=(random_number, 3)) 
            time.sleep(0.1)
        
        feature_store_node = FeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)
        
        time.sleep(0.5)
        for row, sample in enumerate(feature_store_node.get_current_feature_vectors()):
            if row == 0:
                self.assertTrue(np.array_equal(sample, np.array([0., 0., 0.])))

            elif row == 1:
                self.assertTrue(np.array_equal(sample, np.array([1., 1., 1.])))

            """
            elif row == 81:
                self.assertTrue(np.array_equal(sample, np.array([78., 78., 78.])))
            
            elif row == 82:
                self.assertTrue(np.array_equal(sample, np.array([79., 79., 79.])))
            """
            
            if 80 <= row <= 90:
                print(sample)

        self.tearDown_node(feature_store_node)

    def test_save_feature_vector(self):
        feature_store_node = FeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)

        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            time.sleep(0.1)

        self.tearDown_node(feature_store_node)

    def test_many_iterations(self):
        feature_store_node = FeatureStoreNode(name=f"{self._testMethodName}", path=self.path)
        self.start_node(feature_store_node)
        
        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            time.sleep(0.1)
        
        feature_store_node.event.set()
        feature_store_node.log("Starting second iteration")

        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            time.sleep(0.1)

        feature_store_node.event.set()
        feature_store_node.log("Starting third iteration")

        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
            time.sleep(0.1)

        self.tearDown_node(feature_store_node)


if __name__ == "__main__":
    unittest.main()