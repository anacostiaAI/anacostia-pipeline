import unittest
import logging
import sys
import os
import shutil
import time
sys.path.append(os.path.abspath('..'))
sys.path.append(os.path.abspath('../anacostia_pipeline'))

from test_utils import get_log_messages
from anacostia_pipeline.engine.node import BaseNode, AndNode
from anacostia_pipeline.engine.constants import Status

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
        super().__init__(methodName)
    
    def test_get_name(self):
        node = BaseNode("base", "resource")
        self.assertEqual(node.get_name(), "base")

    def test_logger(self):
        node = BaseNode("base", "resource")
        node.set_logger(logger)
        self.assertIs(node.logger, logger)

        message = "hello there"
        node.log(message)

        log_messages = get_log_messages(log_path)
        self.assertEqual(log_messages[0], message)
    
    def test_queue(self):
        node = BaseNode("base", "resource")
        signal = node.signal_message_template()
        signal["status"] = Status.SUCCESS
        queue = node.get_queue()
        
        for i in range(10):
            queue.put(signal)
        
        node.clear_queue(queue)
        self.assertTrue(queue.empty())

if __name__ == "__main__":
   unittest.main()