from multiprocessing import Process, Queue
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

                        if len(signals) == len(self.children):
                            if all(signals.values()):
                                result = self.trigger()
                                if result:
                                    signals = {}
                                    self.send_signal()
                        
                    print(f"Received message: {signal}")

        except KeyboardInterrupt:
            print("Exiting...")
            exit(0)

    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        print(f"Node '{self.name}' setup complete")

        print(f"Starting node '{self.name}'")
        self.__listen()
    
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
    def resource1():
        time.sleep(1)
        print("resource1 triggered")
        return True
    
    def resource2():
        time.sleep(1)
        print("resource2 triggered")
        return True

    node1 = Node("resource1", resource1)
    node2 = Node("resource2", resource2, listen_to=[node1])

    node1.setup()
    #node2.setup()