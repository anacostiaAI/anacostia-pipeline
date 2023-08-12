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
        n5 = HitAPIServer(name='n5', listen_to=n4)
        
        # ---------------

        n6 = ListenHTTP(name='n6')
        # n7 = SendStats(name='n7')


        # p1 and p2 are inherently connected by n5 hitting an endpoint on n6
        # p1 and p2 run on different devices, but connected over network, etc....
        p1 = Pipeline(nodes=[n1, n2, n3, n4, n5], master_server=...)
        p2 = Pipeline(nodes=[n6], master_server=...)

        # subsequeny pipeline deployments
        p1.

        m = MasterPipeline(p1)
        for user in users:
            m.add(user.pipeline)
        
        # How would this run on different devices?
        # defined on one machine, but we have mobile, embedded etc...

        # deplay p1
        Thread(run=p1.start).start()

        for user in users:
            x = p2.serialize()
            user.push(x)

        # pipelines are blackbox together

        # deploy p2 to all end users
        # push_update_or_first_time_install(user_list, p2)
        # mobile app downloads p2 from server p1 lives on or some other server


        assert n3.triggered

if __name__ == "__main__":
    unittest.main()