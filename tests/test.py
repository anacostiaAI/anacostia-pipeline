import unittest
import logging
import sys
import os
import shutil
import time
sys.path.append('..')
sys.path.append('../anacostia_pipeline')

from anacostia_pipeline.resource.filesystem import DirWatchNode
from anacostia_pipeline.engine.node import ActionNode
from anacostia_pipeline.engine.dag import DAG

from test_utils import get_log_messages, create_file, get_time_delta, get_time, delete_file


if os.path.exists("./testing_artifacts") is False:
    os.makedirs("./testing_artifacts")

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
    def setUp(self) -> None:
        try:
            os.makedirs("./testing_artifacts/dirwatchnode")
            os.chmod("./testing_artifacts/dirwatchnode", 0o777)
        except OSError as e:
            print(f"Error occurred: {e}")

        dirwatchnode = DirWatchNode(
            name="testing_artifacts", 
            path="/Users/minhquando/Desktop/anacostia/tests/testing_artifacts/dirwatchnode",
            logger=logger
        )
        dirwatchnode.start()

    def test_init(self):
        time.sleep(2)
        create_file(file_path="./testing_artifacts/dirwatchnode/test.txt", content="This is the content of the file.\n")
        time.sleep(2) 
        delete_file("./testing_artifacts/dirwatchnode/test.txt")
        time.sleep(0.5)

        times = get_time(log_path="./testing_artifacts/app.log", log_level="INFO")
        time_delta = get_time_delta(times[3], times[4])
        self.assertEqual(2.0, time_delta) 

    def tearDown(self) -> None:
        try:
            shutil.rmtree("./testing_artifacts/dirwatchnode")
        except OSError as e:
            print(f"Error occurred: {e}")


if __name__ == '__main__':
   unittest.main()