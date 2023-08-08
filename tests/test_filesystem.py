import unittest
import logging
import sys
import os
import shutil
import time
from multiprocessing import Process, Queue
sys.path.append('..')
sys.path.append('../anacostia_pipeline')

from anacostia_pipeline.resource.filesystem import DirWatchNode
from anacostia_pipeline.engine.node import ActionNode
from anacostia_pipeline.engine.dag import DAG
from anacostia_pipeline.engine.constants import Status

from test_utils import get_log_messages, create_file, get_time_delta, get_time, delete_file, run_node


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
    def __init__(self, methodName: str = "runTest") -> None:
        try:
            os.makedirs("./testing_artifacts/dirwatchnode")
            os.chmod("./testing_artifacts/dirwatchnode", 0o777)
        except OSError as e:
            print(f"Error occurred: {e}")

        self.dirwatchnode = DirWatchNode(
            name="testing_artifacts", 
            path="./testing_artifacts/dirwatchnode",
            logger=logger
        )

        super().__init__(methodName)

        self.process, self.resume_flag = run_node(self.dirwatchnode)

    def setUp(self) -> None:
        try:
            os.makedirs("./testing_artifacts/dirwatchnode")
            os.chmod("./testing_artifacts/dirwatchnode", 0o777)
        except OSError as e:
            print(f"Error occurred: {e}")

    """
    def test_init(self):
        time.sleep(2)
        create_file(file_path="./testing_artifacts/dirwatchnode/test.txt", content="This is the content of the file.\n")
        time.sleep(2) 
        delete_file("./testing_artifacts/dirwatchnode/test.txt")
        time.sleep(0.5)

        times = get_time(log_path="./testing_artifacts/app.log", log_level="INFO")
        time_delta = get_time_delta(times[3], times[4])
        self.assertEqual(2.0, time_delta) 
    """

    def test_initial_message_template(self):
        for i in range(5):
            path = f"./testing_artifacts/dirwatchnode/test_{i}.txt"
            create_file(file_path=path, content="This is the content of the file.\n")
        
        signal = self.dirwatchnode.signal_message_template()
        #print(signal)
        self.assertEqual(5, len(signal["added_files"]))
        self.assertEqual(5, len(signal["modified_files"]))
        #self.assertEqual(0, len(signal["removed_files"]))
        time.sleep(1)
        
    def tearDown(self) -> None:
        self.resume_flag.value = int(Status.STOPPING)
        self.process.join()
        try:
            shutil.rmtree("./testing_artifacts/dirwatchnode")
        except OSError as e:
            print(f"Error occurred: {e}")


if __name__ == '__main__':
   unittest.main()