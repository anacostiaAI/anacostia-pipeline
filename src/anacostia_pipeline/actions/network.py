from logging import Logger
from typing import List
from anacostia_pipeline.engine.base import BaseActionNode, BaseNode
from anacostia_pipeline.engine.constants import Result



class SenderNode(BaseActionNode):
    def __init__(self, name: str, leaf_url: str, predecessors: List[BaseNode], loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, predecessors, loggers)
        self.leaf_url = leaf_url
        self.service = None
    
    def signal_successors(self, result: Result):
        self.service.signal_successors(self, result)



class ReceiverNode(BaseActionNode):
    def __init__(self, name: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, [], loggers)     # Note: ReceiverNodes have no predecessors because they are the root nodes in a leaf DAG
        self.root_url: str = None
        self.service = None

    def signal_predecessors(self, result: Result):
        pass