from typing import List
import time
from logging import Logger

if __name__ == "__main__":
    from base import BaseActionNode, BaseNode
    from anacostia_pipeline.utils.constants import Result
else:
    from engine.base import BaseActionNode, BaseNode, Message
    from anacostia_pipeline.utils.constants import Result



class AndAndNode(BaseNode):
    """
    AndAndNode does the following: 
        1. waits for all predecessors before signalling all successors.
        2. waits for all successors before signalling all predecessors.
    It is useful for synchronizing ActionNodes to wait for all predecessors to finish before executing.
    It is also useful for synchronizing ActionNodes with ResourceNodes 
    to ensure all ActionNodes finish executing before ResourceNodes update their respective state.
    """
    def __init__(self, name: str, predecessors: List[BaseNode], logger: Logger = None) -> None:
        super().__init__(name, predecessors, logger)

    def run(self):
        while True:
            while self.check_predecessors_signals() is False:
                time.sleep(0.1)

            self.log("all resource nodes have finished updating its state.")
            self.signal_successors(Result.SUCCESS)

            # checking for successors signals before signalling predecessors will 
            # ensure all action nodes have finished using the current state
            while self.check_successors_signals() is False:
                time.sleep(0.1)
            
            self.log("all action nodes have finished using the current state.")
            self.signal_predecessors(Result.SUCCESS)


class AndOrNode(BaseActionNode):
    pass


class OrAndNode(BaseNode):
    def __init__(self, name: str, predecessors: List[BaseNode], logger: Logger = None) -> None:
        super().__init__(name, predecessors, logger)

    def check_predecessors_signals(self) -> bool:
        if len(self.predecessors) > 0:
            if self.predecessors_queue.empty():
                return False

            # Pull out the queued up incoming signals and register them
            while not self.predecessors_queue.empty():
                sig: Message = self.predecessors_queue.get()

                if sig.sender not in self.received_predecessors_signals:
                    self.received_predecessors_signals[sig.sender] = sig.result
                else:
                    if self.received_predecessors_signals[sig.sender] != Result.SUCCESS:
                        self.received_predecessors_signals[sig.sender] = sig.result
                # TODO For signaling over the network, this is where we'd send back an ACK

            # Check if the signals match the execute condition
            if len(self.received_predecessors_signals) == len(self.predecessors):
                if any([sig == Result.SUCCESS for sig in self.received_predecessors_signals.values()]):

                    # Reset the received signals
                    self.received_predecessors_signals = dict()
                    return True
                else:
                    return False
            else:
                return False
 
        # If there are no dependent nodes, then we can just return True
        return True

    def run(self):
        while True:
            super().trap_interrupts()
            while self.check_predecessors_signals() is False:
                time.sleep(0.2)
                super().trap_interrupts()

            super().trap_interrupts()
            self.log("all resource nodes have finished updating its state.")
            self.signal_successors(Result.SUCCESS)

            super().trap_interrupts()
            while self.check_successors_signals() is False:
                time.sleep(0.2)
            
            super().trap_interrupts()
            self.log("all action nodes have finished using the current state.")
            self.signal_predecessors(Result.SUCCESS)

class OrOrNode(BaseActionNode):
    pass