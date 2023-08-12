import time

import unittest

from anacostia_pipeline.engine.node import TrueNode, FalseNode
from anacostia_pipeline.engine.pipeline import Pipeline

class PipelineTest(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def test_basic_pipeline(self):
        n1 = TrueNode(name='n1')
        n2 = FalseNode(name='n2')
        n3 = TrueNode(name='n3', listen_to=n1 | n2)
        n4 = TrueNode(name="n4", listen_to=n3)

        p = Pipeline(nodes=[n1, n2, n3, n4])
        p.start()
        time.sleep(1)

        assert n3.triggered

if __name__ == "__main__":
    unittest.main()