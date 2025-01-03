from logging import Logger
from typing import List
import threading
import asyncio

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.utils.constants import Result, Status
from anacostia_pipeline.nodes.network.sender.app import SenderApp



class SenderNode(BaseNode):
    def __init__(
        self, name: str, leaf_host: str, leaf_port: int, leaf_receiver: str, predecessors: List[BaseNode], loggers: Logger | List[Logger] = None
    ) -> None:

        super().__init__(name, predecessors, loggers)
        self.leaf_host = leaf_host
        self.leaf_port = leaf_port
        self.leaf_receiver = leaf_receiver
        self.wait_receiver_node = threading.Event()    # Event to wait for the successor node to signal over the network
        self.app = SenderApp(self, leaf_host, leaf_port, leaf_receiver)
    
    def get_app(self):
        return self.app
    
    async def signal_successors(self, result: Result):
        return await self.app.signal_successors(result)

    def wait_for_successors(self):
        self.wait_receiver_node.wait()
        self.wait_receiver_node.clear()

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