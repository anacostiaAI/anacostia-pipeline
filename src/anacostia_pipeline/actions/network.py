from logging import Logger
from typing import List
import threading
import time
import asyncio

from anacostia_pipeline.engine.base import BaseActionNode, BaseNode
from anacostia_pipeline.engine.constants import Result, Work
from anacostia_pipeline.dashboard.subapps.network import SenderNodeApp, ReceiverNodeApp



class SenderNode(BaseNode):
    def __init__(
        self, name: str, leaf_host: str, leaf_port: int, leaf_receiver: str, predecessors: List[BaseNode], loggers: Logger | List[Logger] = None
    ) -> None:

        super().__init__(name, predecessors, loggers)
        self.leaf_host = leaf_host
        self.leaf_port = leaf_port
        self.leaf_receiver = leaf_receiver
        self.leaf_signal_received = False
        self.wait_successor_event = threading.Event()
        self.shutdown_event = threading.Event()
        self.app = SenderNodeApp(self, leaf_host, leaf_port, leaf_receiver)
    
        self.leaf_pipeline_id: str = None

    def get_app(self):
        return self.app
    
    def set_leaf_pipeline_id(self, pipeline_id: str):
        self.leaf_pipeline_id = pipeline_id

    async def signal_successors(self, result: Result):
        await self.app.signal_successors(result)

    def exit(self):
        self.wait_successor_event.set()
        self.shutdown_event.set()
        super().exit()
    
    async def run_async(self) -> None:
        while self.shutdown_event.is_set() is False:
            self.trap_interrupts()
            self.work_list.append(Work.WAITING_PREDECESSORS)
            while self.check_predecessors_signals() is False:
                time.sleep(0.2)
                self.trap_interrupts()
            self.work_list.remove(Work.WAITING_PREDECESSORS)

            self.trap_interrupts()
            await self.signal_successors(Result.SUCCESS)

            self.trap_interrupts()
            self.work_list.append(Work.WAITING_SUCCESSORS)
            self.wait_successor_event.wait()
            self.work_list.remove(Work.WAITING_SUCCESSORS)
            
            # self.log(f"SenderNode {self.name} received signal from successors", level="INFO")

            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS)

            self.wait_successor_event.clear()

    def run(self) -> None:
        asyncio.run(self.run_async())


class ReceiverNode(BaseNode):
    def __init__(self, name: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, [], loggers)     # Note: ReceiverNodes have no predecessors because they are the root nodes in a leaf DAG
        self.root_url: str = None
        self.service = None
        
        self.wait_predecessor_event = threading.Event()
        self.shutdown_event = threading.Event()

        self.app = ReceiverNodeApp(self)

    def get_app(self):
        return self.app
    
    async def signal_predecessors(self, result: Result):
        self.log(f"Signaling predecessors", level="INFO")
        await self.app.signal_predecessors(result)
    
    def exit(self):
        self.wait_predecessor_event.set()
        self.shutdown_event.set()
        super().exit()
    
    async def run_async(self) -> None:
        # design a new node lifecycle for ReceiverNode
        # the node lifecycle for ReceiverNode should:
        # 1. wait for a signal (an http post request) from the SenderNode
        # 2. signal the successors
        # 3. wait for signals from the successors
        # 4. signal the predecessor (the SenderNode) via http post request to the root_url  
        # 5. repeat steps 1-4 until the pipeline is terminated
        # Note: the run method should be able to execute asynchronous calls

        while self.shutdown_event.is_set() is False:
            self.work_list.append(Work.WAITING_PREDECESSORS)
            self.wait_predecessor_event.wait()

            self.work_list.remove(Work.WAITING_PREDECESSORS)
            self.wait_predecessor_event.clear()

            self.signal_successors(Result.SUCCESS)

            self.trap_interrupts()
            self.work_list.append(Work.WAITING_SUCCESSORS)
            while self.check_successors_signals() is False:
                time.sleep(0.2)
                self.trap_interrupts()
            self.work_list.remove(Work.WAITING_SUCCESSORS)

            self.trap_interrupts()
            # await self.signal_predecessors(Result.SUCCESS)

    def run(self) -> None:
        asyncio.run(self.run_async())