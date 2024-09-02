from logging import Logger
from typing import List
import threading
import time

from anacostia_pipeline.engine.base import BaseActionNode, BaseNode
from anacostia_pipeline.engine.constants import Result, Work
from anacostia_pipeline.dashboard.subapps.network import SenderNodeApp, ReceiverNodeApp



class SenderNode(BaseActionNode):
    def __init__(
        self, name: str, leaf_host: str, leaf_port: int, leaf_receiver: str, predecessors: List[BaseNode], loggers: Logger | List[Logger] = None
    ) -> None:

        super().__init__(name, predecessors, loggers)
        self.leaf_host = leaf_host
        self.leaf_port = leaf_port
        self.leaf_receiver = leaf_receiver
        self.leaf_signal_received = False
        self.app = SenderNodeApp(self, leaf_host, leaf_port, leaf_receiver)
    
        self.leaf_pipeline_id: str = None

    def get_app(self):
        return self.app
    
    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    """
    def signal_successors(self, result: Result):
        self.app.signal_successors(result)
        self.log(f"{self.name} signaled successors", level="INFO")
    """
    
    def signal_successors(self, result: Result):
        super().signal_successors(result)
        self.log(f"{self.name} signaled successors", level="INFO")
    
    """
    def check_successors_signals(self) -> bool:
        return self.leaf_signal_received
    """
    
    def signal_predecessors(self, result: Result):
        self.leaf_signal_received = False
        super().signal_predecessors(result)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing SenderNode {self.name}", level="INFO")
        return True



class ReceiverNode(BaseActionNode):
    def __init__(self, name: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, [], loggers)     # Note: ReceiverNodes have no predecessors because they are the root nodes in a leaf DAG
        self.root_url: str = None
        self.service = None
        self.event = threading.Event()
        self.app = ReceiverNodeApp(self)

    def get_app(self):
        return self.app
    
    def signal_predecessors(self, result: Result):
        self.log(f"Signaling predecessors", level="INFO")
        self.app.signal_predecessors(result)
    
    def execute(self, *args, **kwargs) -> bool:
        self.log(f"Executing ReceiverNode {self.name}", level="INFO")
        return True
    
    def run(self) -> None:
        # design a new node lifecycle for ReceiverNode
        # the node lifecycle for ReceiverNode should:
        # 1. wait for a signal (an http post request) from the SenderNode
        # 2. signal the successors
        # 3. wait for signals from the successors
        # 4. signal the predecessor (the SenderNode) via http post request to the root_url  
        # 5. repeat steps 1-4 until the pipeline is terminated
        # Note: the run method should be able to execute asynchronous calls

        self.work_list.append(Work.WAITING_PREDECESSORS)
        self.event.wait()
        self.work_list.remove(Work.WAITING_PREDECESSORS)

        self.signal_successors(Result.SUCCESS)

        # checking for successors signals before signalling predecessors will 
        # ensure all action nodes have finished using the resource for current run
        self.trap_interrupts()
        self.work_list.append(Work.WAITING_SUCCESSORS)
        while self.check_successors_signals() is False:
            time.sleep(0.2)
            self.trap_interrupts()
        self.work_list.remove(Work.WAITING_SUCCESSORS)