import time
import sys
import unittest

sys.path.append('..')
sys.path.append('../anacostia_pipeline')
from anacostia_pipeline.engine.node import TrueNode, FalseNode
from anacostia_pipeline.engine.pipeline import Pipeline

class PipelineTest(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def test_triggering_pipeline(self):
        n1 = TrueNode(name='n1')
        n2 = FalseNode(name='n2')
        n3 = TrueNode(name='n3', listen_to=n1 | n2)
        n4 = TrueNode(name="n4", listen_to=n3)
        n5 = FalseNode(name="n5", listen_to=n3 & n2)
        n6 = TrueNode(name="n6", listen_to=n3 & n2)

        #p1 = Pipeline(nodes=[n1, n2, n3, n4, n5])
        p1 = Pipeline(nodes=[n1, n2, n3, n4, n5, n6])
        p1.start()

        time.sleep(2)

        # the reason why all nodes are triggered is because i set the auto_trigger to True by default
        assert n1.triggered
        assert not n2.triggered
        assert n3.triggered
        assert n4.triggered 
        assert not n5.triggered
        assert n6.triggered

        p1.terminate_nodes()

    def test_pause(self):
        n1 = TrueNode(name='n1')
        n2 = FalseNode(name='n2')
        n3 = TrueNode(name='n3', listen_to=n1 | n2)
        n4 = TrueNode(name="n4", listen_to=n3)
        n5 = FalseNode(name="n5", listen_to=n3 & n2)

        p1 = Pipeline(nodes=[n1, n2, n3, n4, n5])
        p1.start()

        n4.pause()

        time.sleep(2)

        assert n1.triggered
        assert not n2.triggered
        assert n3.triggered
        assert n4.triggered 
        assert not n5.triggered

        p1.terminate_nodes()

if __name__ == "__main__":
    unittest.main()