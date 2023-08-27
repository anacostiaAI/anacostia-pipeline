import time

import unittest

from anacostia_pipeline.engine.node import TrueNode, FalseNode
from anacostia_pipeline.engine.pipeline import Pipeline

class PipelineTest(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def test_triggering_pipeline(self):
        n1 = TrueNode(name='n1', auto_trigger=True, retrigger=False)
        n2 = FalseNode(name='n2', auto_trigger=True, retrigger=False)
        n3 = TrueNode(name='n3', listen_to=n1 | n2, retrigger=False)
        n4 = TrueNode(name="n4", listen_to=n3, retrigger=False)
        n5 = FalseNode(name="n5", listen_to=n3 & n2, retrigger=False)

        p1 = Pipeline(nodes=[n1, n2, n3, n4, n5])
        p1.start()

        time.sleep(8)

        assert n1.triggered
        assert n2.triggered
        assert n3.triggered
        assert n4.triggered 
        assert not n5.triggered

        p1.terminate_nodes()

    def test_pause(self):
        n1 = TrueNode(name='n1', auto_trigger=True, retrigger=False)
        n2 = FalseNode(name='n2', auto_trigger=True, retrigger=False)
        n3 = TrueNode(name='n3', listen_to=n1 | n2, retrigger=False)
        n4 = TrueNode(name="n4", listen_to=n3, retrigger=False)
        n5 = FalseNode(name="n5", listen_to=n3 & n2, retrigger=False)

        p1 = Pipeline(nodes=[n1, n2, n3, n4, n5])
        p1.start()

        n4.pause()

        time.sleep(8)

        assert n1.triggered
        assert n2.triggered
        assert n3.triggered
        assert not n4.triggered 
        assert not n5.triggered

        p1.terminate_nodes()

if __name__ == "__main__":
    unittest.main()