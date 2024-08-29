from logging import Logger
from typing import List
from anacostia_pipeline.engine.base import BaseActionNode, BaseNode
from anacostia_pipeline.engine.constants import Result
from anacostia_pipeline.dashboard.subapps.network import SenderNodeApp, ReceiverNodeApp



class SenderNode(BaseActionNode):
    def __init__(self, name: str, leaf_host: str, leaf_port: int, leaf_receiver, predecessors: List[BaseNode], loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, predecessors, loggers)
        self.leaf_host = leaf_host
        self.leaf_port = leaf_port
        self.leaf_receiver = leaf_receiver
        self.leaf_signal_received = False
        self.app = SenderNodeApp(self, leaf_host, leaf_port)
    
    def get_app(self):
        return self.app
    
    def signal_successors(self, result: Result):
        self.app.signal_successors(result)
    
    def check_successors_signals(self) -> bool:
        return self.leaf_signal_received
    
    def signal_predecessors(self, result: Result):
        self.leaf_signal_received = False
        return super().signal_predecessors(result)
    
    def execute(self, *args, **kwargs) -> bool:
        return True



class ReceiverNode(BaseActionNode):
    def __init__(self, name: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, [], loggers)     # Note: ReceiverNodes have no predecessors because they are the root nodes in a leaf DAG
        self.root_url: str = None
        self.service = None
        self.app = ReceiverNodeApp(self)

    def get_app(self):
        return self.app
    
    def signal_predecessors(self, result: Result):
        self.app.signal_predecessors(result)
    
    def execute(self, *args, **kwargs) -> bool:
        return True