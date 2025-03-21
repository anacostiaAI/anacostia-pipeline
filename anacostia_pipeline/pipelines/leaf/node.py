import asyncio
from anacostia_pipeline.nodes.node import BaseNode
from logging import Logger
from typing import List, Union



class LeafConnectionNode(BaseNode):
    def __init__(self, name: str, loggers: Union[Logger, List[Logger]] =None):
        super().__init__(name=name, loggers=loggers)
    
    async def run_async(self):
        self.log(f'{self.name} waiting for root predecessors to connect')

        while len(self.predecessors_events) <= 0:
            await asyncio.sleep(0.1)
            if self.exit_event.is_set(): return

        self.log(f'{self.name} connected to root predecessors {list(self.predecessors_events.keys())}')

        while self.exit_event.is_set() is False:

            if self.exit_event.is_set(): return
            self.wait_for_predecessors()

            if self.exit_event.is_set(): return
            await self.signal_successors()

            if self.exit_event.is_set(): return
            self.wait_for_successors()

            if self.exit_event.is_set(): return
            await self.signal_predecessors()