from multiprocessing import Queue
from threading import Thread
from typing import List, Any, Callable, Dict
import time
import networkx as nx
from constants import Status


G = nx.DiGraph()


class BaseNode:
    queue = Queue()

    def __init__(self, 
        name: str, 
        signal_type: str,
        action_function: Callable[..., Any] = None, 
        listen_to: List['BaseNode'] = [],
        auto_trigger: bool = False
    ) -> None:

        self.name = name
        self.signal_type = signal_type
        self.action_function = action_function
        self.triggered = False
        self.children = listen_to
        self.auto_trigger = auto_trigger
        self.signals_received = {child.get_name():Status.WAITING for child in self.children}

        G.add_node(self)

        for child in self.children:
            G.add_edge(child, self, signal_type=self.signal_type)

    def __hash__(self) -> int:
        return hash(self.name)

    def get_name(self) -> str:
        return self.name

    def get_queue(self) -> Queue:
        return self.queue

    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        print(f"Node '{self.name}' setup complete")

    def precheck(self) -> bool:
        # should be used for continuously checking if the node is ready to start
        # i.e., checking if database connections, API connections, etc. are ready 
        return True

    def pre_execution(self) -> None:
        # override to enable node to do something before execution; 
        # e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        pass

    def on_success(self) -> None:
        # override to enable node to do something after execution in event of success of action_function; 
        # e.g., send an email to the data science team to let everyone know the pipeline has finished training a new model
        pass

    def on_failure(self) -> None:
        # override to enable node to do something after execution in event of failure of action_function; 
        # e.g., send an email to the data science team to let everyone know the pipeline has failed to train a new model
        pass
    
    def teardown(self) -> None:
        print(f"Node '{self.name}' teardown complete")
    
    def __repr__(self) -> str:
        return f"'Node({self.name})'"

    def __send_signal(self, status: Status) -> None:
        print(f"Sending signal from node '{self.name}'")
        signal = {
            "name": self.name,
            "signal_type": self.signal_type,
            "timestamp": time.time(),
            "status": status
        }
        self.queue.put(signal)
    
    def __execution(self) -> bool:
        try:
            if self.action_function is not None:
                self.pre_execution()
                self.action_function()
                self.on_success()
            return True
        
        except Exception as e:
            print(f"Node '{self.name}' execution failed: {e}")
            self.on_failure()
            return False

    def __reset_trigger(self) -> None:
        self.triggered = False
        if len(self.children) > 0:
            self.signals_received = {child.get_name():Status.WAITING for child in self.children}
    
    def __clear_queue(self, queue: Queue) -> Dict[str, Any]:
        # sleep for a short time to allow queue to be populated
        # time.sleep(0.1)
        signal = queue.get()
        while not queue.empty():
            if signal["status"] == Status.SUCCESS:
                queue.get()
            else:
                signal = queue.get()
        return signal

    def poll_children(self) -> bool:
        if len(self.children) == 0:
            return True
        else:
            for child in self.children:
                queue = child.get_queue()
                signal = self.__clear_queue(queue)
            
                if self.signals_received[child.get_name()] == Status.WAITING:
                    self.signals_received[child.get_name()] = signal["status"]
                    print(f"Received signal '{signal}' from node '{child.get_name()}'")
            
            if all(value == Status.SUCCESS for value in self.signals_received.values()) is True:
                return True

        return False

    def trigger(self) -> None:
        # method called by user to manually trigger node
        if self.precheck() is True:
            self.triggered = True

    def __set_auto_trigger(self) -> None:
        if self.auto_trigger is True:
            if self.precheck() is True:
                if self.poll_children() is True:
                    self.triggered = True

    def __run(self) -> None:
        try:
            self.setup()
            # keep in mind that when initializing leaf node, parent nodes may not be initialized yet
            # so some initial signals sent from leaf node may not be received by parent nodes

            while True:
                self.__set_auto_trigger()

                if self.triggered is True:
                    if self.__execution() is True:
                        self.__reset_trigger()
                        self.__send_signal(Status.SUCCESS)

        except KeyboardInterrupt:
            self.teardown()
            exit(0)

    def start(self) -> None:
        print(f"Starting node '{self.name}'")
        proc_listen = Thread(target=self.__run, daemon=True)
        proc_listen.start()


class AndNode(BaseNode):
    def __init__(self, listen_to: List['BaseNode']) -> None:
        if len(listen_to) < 2:
            raise ValueError("node '{name}' must be listening to at LEAST two nodes; listen_to must be provided")
        
        super().__init__("AND", "AND", action_function=None, listen_to=listen_to, auto_trigger=True)


class OrNode(BaseNode):
    def __init__(self, listen_to: List['BaseNode']) -> None:
        if len(listen_to) < 2:
            raise ValueError("node '{name}' must be listening to at LEAST two nodes; listen_to must be provided")

        super().__init__("OR", "OR", action_function=None, listen_to=listen_to, auto_trigger=True)

    def poll_children(self) -> bool:
        for child in self.children:
            queue = child.get_queue()
            signal = self.__clear_queue(queue)
        
            if signal == Status.SUCCESS:
                return True
        
        return False


class ResourceNode(BaseNode):
    def __init__(self, name: str) -> None:
        super().__init__(name, "resource")
    
    
class ActionNode(BaseNode):
    def __init__(self, 
        name: str, 
        signal_type: str, 
        action_function: Callable[..., Any], 
        listen_to: List['BaseNode'] = []
    ) -> None:
        
        super().__init__(name, signal_type, action_function, listen_to, auto_trigger=True)


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

    node1 = ActionNode("resource1", "preprocess", resource1)
    node2 = ActionNode("resource2", "load_model", resource2)
    node3 = ActionNode("train_model", "train", train_model, listen_to=[node1, node2])

    sorted_nodes = nx.topological_sort(G)
    for node in sorted_nodes:
        node.start()

    time.sleep(10)

    # we can use the to_dict_of_dicts function to output the graph as a json file
    graph = nx.to_dict_of_dicts(G)
    print(graph)