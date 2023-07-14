from multiprocessing import Process, Queue
from typing import List, Dict, Any, Callable
import time


class Node:
    queue = Queue()
    subscriptions_queues = []

    def __init__(self, name: str, listen_to: List = []) -> None:
        self.name = name

        self.children = listen_to
        for child in listen_to:
            self.subscriptions_queues.append(child.get_queue())

    def get_queue(self) -> Queue:
        return self.queue
    
    def _setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        time.sleep(1)
        print(f"Node '{self.name}' setup complete")
    
    def __repr__(self) -> str:
        return f"Node({self.name})"

"""
class Node:
    # in the future, replace this queue with an event streaming log like RabbitMQ, Apache Kafka, or ZeroMQ
    queue = Queue()
    subscriptions = []

    def __init__(
        self, 
        subscriptions: List = None
    ) -> None:
        
        # we can enable a node to subscribe to other nodes by having it listen to the other nodes' queues
        if subscriptions is not None:
            for subscription in subscriptions:
                self.subscriptions.append(subscription.get_queue())

    def get_queue(self) -> Queue:
        return self.queue

    # Function to receive messages
    def receiver(queue):
        while True:
            msg = queue.get()

            if msg is None:
                break

            print(f"Received message: {msg}")

    def _setup(self) -> None:
        self._get_tools()
        #proc = Process(target=self._listen, daemon=True)
        #proc.start()

    def _trigger(self) -> bool:
        proc = Process(target=self.trigger_function)
        proc.start()
        proc.join()

    def _listen(self) -> None:
        while True:
            # if signal received, call _trigger
            break
    
    def _send_signal(self) -> None:
        pass

    def _get_tools(self) -> None:
        proc = Process(target=self.get_tools, daemon=True)
        proc.start()
        proc.join()

    def get_tools(self) -> None:
        print("Downloading tools")
        time.sleep(1)
        print("Installed tools")

    def trigger_function(self) -> bool:
        return True
"""


if __name__ == "__main__":
    node = Node()
    node._setup()