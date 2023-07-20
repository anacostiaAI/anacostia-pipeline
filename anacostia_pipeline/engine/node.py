from multiprocessing import Process, Queue
from threading import Thread
from typing import List, Dict, Any, Callable
import time
import networkx as nx


G = nx.DiGraph()


class BaseNode:
    queue = Queue()

    def __init__(self, 
        name: str, 
        signal: str,
        listen_to: List['Node'] = []
    ) -> None:

        self.name = name
        self.signal = signal
        self.children = listen_to

        G.add_node(self)
        for child in self.children:
            # we can add information about signals, e.g. signal type, signal value, etc.
            # docker information, e.g. docker image, docker container, etc.
            # and whatever other information needed to recreate the environment and the DAG using the add_edge function
            G.add_edge(child, self, signal=self.signal)
    
    def get_name(self) -> str:
        return self.name

    def get_queue(self) -> Queue:
        return self.queue

    def send_signal(self) -> None:
        print(f"Sending signal from node '{self.name}'")
        self.queue.put(self.signal)
    
    def __hash__(self) -> int:
        return hash(self.name)

    def listen(self) -> None:
        raise NotImplementedError

    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        print(f"Node '{self.name}' setup complete")
    
    def start(self) -> None:
        self.setup()
        # keep in mind that when initializing leaf node, parent nodes may not be initialized yet
        # so some initial signals sent from leaf node may not be received by parent nodes

        print(f"Starting node '{self.name}'")
        proc_listen = Thread(target=self.listen, daemon=True)
        proc_listen.start()

    def teardown(self) -> None:
        print(f"Node '{self.name}' teardown complete")
    
    def __repr__(self) -> str:
        return f"Node({self.name})"

class ResourceNode(BaseNode):
    def __init__(self, name: str) -> None:
        super().__init__(name, "resource_signal")
    

class Node:
    queue = Queue()

    def __init__(self, 
        name: str, 
        signal_type: str,
        action_function: Callable[..., any] = None, 
        listen_to: List['Node'] = []
    ) -> None:

        self.name = name
        self.signal_type = signal_type
        self.action_function = action_function
        self.children = listen_to

        G.add_node(self)
        for child in self.children:
            # we can add information about signals, e.g. signal type, signal value, etc.
            # docker information, e.g. docker image, docker container, etc.
            # and whatever other information needed to recreate the environment and the DAG using the add_edge function
            G.add_edge(child, self, signal=self.signal_type)

    def get_queue(self) -> Queue:
        return self.queue

    def send_signal(self) -> None:
        print(f"Sending signal from node '{self.name}'")
        self.queue.put(
            {
                "signal_type": self.signal_type,
                "value": True
            }
        )
    
    def get_signal_value(self, queue_value: Dict) -> str:
        return queue_value["value"]
    
    def trigger(self) -> bool:
        try:
            if self.action_function is not None:
                self.action_function()
            return True

        except Exception as e:
            print(f"Error: {e}")
            return False

    def __hash__(self) -> int:
        return hash(self.name)

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
                            signals[child.name] = self.get_signal_value(signal)
                        else:
                            if (signals[child.name] == False) and (self.get_signal_value(signal) == True):
                                signals[child.name] = self.get_signal_value(signal)
                        
                        print(f"{self.name} received message from {child.name}: {self.get_signal_value(signal)}")

                        if len(signals) == len(self.children):
                            if all(signals.values()):
                                result = self.trigger()
                                if result:
                                    signals = {}
                                    self.send_signal()

        except KeyboardInterrupt:
            print("\nExiting...")
            self.teardown()
            exit(0)

    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        print(f"Node '{self.name}' setup complete")
    
    def start(self) -> None:
        self.setup()
        # keep in mind that when initializing leaf node, parent nodes may not be initialized yet
        # so some initial signals sent from leaf node may not be received by parent nodes

        print(f"Starting node '{self.name}'")
        proc_listen = Thread(target=self.__listen, daemon=True)
        proc_listen.start()
    
    def teardown(self) -> None:
        print(f"Node '{self.name}' teardown complete")
    
    def __repr__(self) -> str:
        return f"'Node({self.name})'"


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

    node1 = Node("resource1", "resource", resource1)
    node2 = Node("resource2", "resource", resource2)
    node3 = Node("train_model", "resource", train_model, listen_to=[node1, node2])

    sorted_nodes = nx.topological_sort(G)
    for node in sorted_nodes:
        node.start()

    time.sleep(5)

    # we can use the to_dict_of_dicts function to output the graph as a json file
    graph = nx.to_dict_of_dicts(G)
    print(graph)