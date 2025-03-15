from logging import Logger
from typing import List
import threading
import asyncio

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.utils.constants import Result, Status
from anacostia_pipeline.nodes.network.receiver.app import ReceiverApp



class ReceiverNode(BaseNode):
    def __init__(self, name: str, loggers: Logger | List[Logger] = None) -> None:
        super().__init__(name, [], loggers)     # Note: ReceiverNodes have no predecessors because they are the root nodes in a leaf DAG
        self.root_url: str = None
        self.service = None
        self.wait_sender_node = threading.Event()

        self.app = ReceiverApp(self)

    def get_app(self):
        return self.app
    
    async def signal_predecessors(self, result: Result):
        await self.app.signal_predecessors(result)
    
    def wait_for_predecessors(self):
        self.wait_sender_node.wait()
        self.wait_sender_node.clear()

    def exit(self):
        self.wait_sender_node.set()
        super().exit()
    
    async def run_async(self) -> None:
        while self.exit_event.is_set() is False:
            self.wait_for_predecessors()

            if self.exit_event.is_set(): return
            self.signal_successors(Result.SUCCESS)

            if self.exit_event.is_set(): return
            self.wait_for_successors()

            if self.exit_event.is_set(): return
            await self.signal_predecessors(Result.SUCCESS)
