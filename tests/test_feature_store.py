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

if os.path.exists("./testing_artifacts/feature_store_tests") is True:
    shutil.rmtree("./testing_artifacts/feature_store_tests")

os.makedirs("./testing_artifacts/feature_store_tests")
os.chmod("./testing_artifacts/feature_store_tests", 0o777)

# Create a logger
log_path = "./testing_artifacts/feature_store_tests/feature_store.log"
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
        self.feature_store_dirs = os.listdir("./testing_artifacts/feature_store_tests")
        self.path = f"./testing_artifacts/feature_store_tests/feature_store_{self._testMethodName}"
        os.makedirs(self.path)

    def test_save_feature_vectors(self):
        feature_store_node = FeatureStoreNode(name=f"feature_store_{self._testMethodName}", path=self.path)
        feature_store_node.set_logger(logger)
        feature_store_node.set_semaphore(value=1)
        feature_store_node.start()
        time.sleep(1)
        
        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
        
        time.sleep(1)
        feature_store_node.stop()
        feature_store_node.join()
    
    def test_setup(self):
        # putting files into the feature_store folder before starting
        os.makedirs(f"{self.path}/feature_store")
        for i in range(5):
            random_number = random.randint(0, 100)
            create_numpy_file(file_path=f"{self.path}/feature_store/features_{i}", shape=(random_number, 3)) 
        
        feature_store_node = FeatureStoreNode(name=f"feature_store_{self._testMethodName}", path=self.path)
        feature_store_node.set_logger(logger)
        feature_store_node.set_semaphore(value=1)
        feature_store_node.start()
        
        time.sleep(1)

        feature_store_node.stop()
        feature_store_node.join()

    """
    def test_get_current_feature_vectors(self):
        feature_store_node = FeatureStoreNode(name=f"feature_store_{self._testMethodName}", path=self.path)
        feature_store_node.set_logger(logger)
        feature_store_node.set_semaphore(value=1)
        feature_store_node.start()
        time.sleep(1)
        
        for _ in range(5):
            random_number = random.randint(0, 100)
            array = create_array(shape=(random_number, 3))
            feature_store_node.save_feature_vector(array)
        
        time.sleep(1)

        for row, sample in enumerate(feature_store_node.get_current_feature_vectors()):
            if row == 0:
                self.assertTrue(np.array_equal(sample, np.array([0., 0., 0.])))

            elif row == 1:
                self.assertTrue(np.array_equal(sample, np.array([1., 1., 1.])))

            elif row == 81:
                self.assertTrue(np.array_equal(sample, np.array([0., 0., 0.])))
            
            elif row == 82:
                self.assertTrue(np.array_equal(sample, np.array([1., 1., 1.])))
            
            if 70 < row < 90:
                print(sample)

        feature_store_node.stop()
        feature_store_node.join()
    """


if __name__ == "__main__":
    unittest.main()