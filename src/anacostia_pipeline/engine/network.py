from logging import Logger
from typing import List
import threading
import asyncio

from anacostia_pipeline.engine.base import BaseNode
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
        self.wait_receiver_node = threading.Event()    # Event to wait for the successor node to signal over the network
        self.app = SenderNodeApp(self, leaf_host, leaf_port, leaf_receiver)
    
    def get_app(self):
        return self.app
    
    async def signal_successors(self, result: Result):
        return await self.app.signal_successors(result)

    def wait_for_successors(self):
        self.work_list.append(Work.WAITING_SUCCESSORS)
        self.wait_receiver_node.wait()
        self.wait_receiver_node.clear()
        self.work_list.remove(Work.WAITING_SUCCESSORS)

    def exit(self):
        self.wait_receiver_node.set()
        super().exit()
    
    async def run_async(self) -> None:
        while self.exit_event.is_set() is False:
            self.wait_for_predecessors()
            
            if self.exit_event.is_set(): break
            await self.signal_successors(Result.SUCCESS)

            # Wait for the successor node to signal over the network
            if self.exit_event.is_set(): break
            self.wait_for_successors()
            
            if self.exit_event.is_set(): break
            self.signal_predecessors(Result.SUCCESS)

    def run(self) -> None:
        asyncio.run(self.run_async())


class ReceiverNode(BaseNode):
    def __init__(self, name: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, [], loggers)     # Note: ReceiverNodes have no predecessors because they are the root nodes in a leaf DAG
        self.root_url: str = None
        self.service = None
        self.wait_sender_node = threading.Event()

        self.app = ReceiverNodeApp(self)

    def get_app(self):
        return self.app
    
    async def signal_predecessors(self, result: Result):
        await self.app.signal_predecessors(result)
    
    def wait_for_predecessors(self):
        self.work_list.append(Work.WAITING_PREDECESSORS)
        self.wait_sender_node.wait()
        self.wait_sender_node.clear()
        self.work_list.remove(Work.WAITING_PREDECESSORS)

    def exit(self):
        self.wait_sender_node.set()
        super().exit()
    
    async def run_async(self) -> None:
        while self.exit_event.is_set() is False:
            self.wait_for_predecessors()

            if self.exit_event.is_set(): break
            self.signal_successors(Result.SUCCESS)

            if self.exit_event.is_set(): break
            self.wait_for_successors()

            if self.exit_event.is_set(): break
            await self.signal_predecessors(Result.SUCCESS)

    def run(self) -> None:
        asyncio.run(self.run_async())