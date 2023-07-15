from multiprocessing import Process, Queue
from threading import Thread
from typing import List, Dict, Any, Callable
import time


class Node:
    queue = Queue()

    def __init__(self, 
        name: str, 
        action_function: Callable[..., any] = None, 
        listen_to: List['Node'] = []
    ) -> None:

        self.name = name
        self.action_function = action_function
        self.children = listen_to

    def get_queue(self) -> Queue:
        return self.queue

    def send_signal(self) -> None:
        print(f"Sending signal from node '{self.name}'")
        self.queue.put(True)
    
    def trigger(self) -> bool:
        try:
            self.action_function()
            return True

        except Exception as e:
            print(f"Error: {e}")
            return False

    def __listen(self) -> None:
        signals = {}

        try:
            while True:

                # if node is not listening for signals from other nodes, 
                # trigger action function (this is done for resource nodes);
                # otherwise, wait for signals from other nodes
                if len(self.children) == 0:
                    result = self.trigger()
                    if result:
                        self.send_signal()

                else:
                    for child in self.children:
                        queue = child.get_queue()
                        signal = queue.get()

                        if child.name not in signals:
                            signals[child.name] = signal
                        else:
                            if (signals[child.name] == False) and (signal == True):
                                signals[child.name] = signal
                        
                        print(f"{self.name} received message from {child.name}: {signal}")

                        if len(signals) == len(self.children):
                            if all(signals.values()):
                                result = self.trigger()
                                if result:
                                    signals = {}
                                    self.send_signal()

        except KeyboardInterrupt:
            print("\nExiting...")
            exit(0)

    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        print(f"Node '{self.name}' setup complete")
        # keep in mind that when initializing leaf node, parent nodes may not be initialized yet
        # so some initial signals sent from leaf node may not be received by parent nodes

        print(f"Starting node '{self.name}'")
        proc_listen = Thread(target=self.__listen, daemon=True)
        proc_listen.start()
    
    def __repr__(self) -> str:
        return f"Node({self.name})"


if __name__ == "__main__":
    def resource1():
        print("checking resource1")
        time.sleep(1)
        print("resource1 triggered")
        return True
    
    def resource2():
        print("checking resource2")
        time.sleep(2)
        print("resource2 triggered")
        return True
    
    def train_model():
        print("train_model triggered")
        time.sleep(3)
        print("train_model finished")
        return True

    node1 = Node("resource1", resource1)
    node2 = Node("resource2", resource2)
    node3 = Node("train_model", train_model, listen_to=[node1, node2])

    node1.setup()
    node2.setup()
    node3.setup()
    time.sleep(10)