from multiprocessing import Queue
from threading import Thread
from typing import List, Any, Callable
import time
import networkx as nx


G = nx.DiGraph()


class BaseNode:
    queue = Queue()

    def __init__(self, 
        name: str, 
        signal_type: str,
        action_function: Callable[..., Any] = None, 
    ) -> None:

        self.name = name
        self.signal_type = signal_type
        self.action_function = action_function
        self.triggered = False

        G.add_node(self)

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

    def __send_signal(self) -> None:
        print(f"Sending signal from node '{self.name}'")
        self.queue.put(self.signal_type)
    
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
    
    def reset_listeners(self) -> None:
        pass

    def __reset_trigger(self) -> None:
        self.triggered = False
        self.reset_listeners()
    
    def trigger(self) -> None:
        # method called by user to manually trigger node
        if self.precheck() is True:
            self.triggered = True
            return

    def set_auto_trigger(self) -> None:
        # override to enable node to automatically set trigger
        pass

    def __run(self) -> None:
        try:
            self.setup()
            # keep in mind that when initializing leaf node, parent nodes may not be initialized yet
            # so some initial signals sent from leaf node may not be received by parent nodes

            while True:
                self.set_auto_trigger()

                if self.triggered is True:
                    #print(f"Node '{self.name}' triggered")
                    if self.__execution() is True:
                        self.__reset_trigger()
                        self.__send_signal()

        except KeyboardInterrupt:
            self.teardown()
            exit(0)

    def start(self) -> None:
        print(f"Starting node '{self.name}'")
        proc_listen = Thread(target=self.__run, daemon=True)
        proc_listen.start()


class ResourceNode(BaseNode):
    def __init__(self, name: str) -> None:
        super().__init__(name, "resource")
    
    
class ActionNode(BaseNode):
    def __init__(self, 
        name: str, 
        signal_type: str, 
        action_function: Callable[..., Any] = None, 
        listen_to: List['ActionNodes'] = []
    ) -> None:
        
        super().__init__(name, signal_type, action_function)

        # we can add information about signals, e.g. signal type, signal value, etc.
        # docker information, e.g. docker image, docker container, etc.
        # and whatever other information needed to recreate the environment and the DAG using the add_edge function
        self.children = listen_to
        self.signals_received = {child.get_name():False for child in self.children}

        for child in self.children:
            G.add_edge(child, self, signal_type=self.signal_type)

    def reset_listeners(self) -> None:
        for name in self.signals_received:
            self.signals_received[name] = False

    def set_auto_trigger(self) -> None:
        if self.precheck() is True:
            if self.__poll_children() is True:
                self.triggered = True
                return

    def __poll_children(self) -> bool:
        if len(self.children) == 0:
            return True
        else:
            for child in self.children:
                queue = child.get_queue()
                signal = queue.get()
            
                if self.signals_received[child.get_name()] is False:
                    self.signals_received[child.get_name()] = True
                    print(f"Received signal '{signal}' from node '{child.get_name()}'")
            
            if all(self.signals_received.values()) is True:
                return True


class ActionNodes:
    queue = Queue()

    def __init__(self, 
        name: str, 
        signal_type: str,
        action_function: Callable[..., any] = None, 
        listen_to: List['ActionNodes'] = []
    ) -> None:

        self.name = name
        self.signal_type = signal_type
        self.action_function = action_function
        self.children = listen_to
        self.triggered = False
        self.signals_received = {child.get_name():False for child in self.children}

        G.add_node(self)
        for child in self.children:
            # we can add information about signals, e.g. signal type, signal value, etc.
            # docker information, e.g. docker image, docker container, etc.
            # and whatever other information needed to recreate the environment and the DAG using the add_edge function
            G.add_edge(child, self, signal_type=self.signal_type)
    
    def get_name(self) -> str:
        return self.name

    def get_queue(self) -> Queue:
        return self.queue

    def __send_signal(self) -> None:
        print(f"Sending signal from node '{self.name}'")
        self.queue.put(self.signal_type)
    
    def trigger(self):
        if self.precheck() is True:
            if self.__poll_resources() is True:
                self.triggered = True
                return
    
    def __reset_trigger(self):
        self.triggered = False
        for name in self.signals_received:
            self.signals_received[name] = False
    
    def __hash__(self) -> int:
        return hash(self.name)
    
    def precheck(self) -> bool:
        # should be used for continuously checking if the node is ready to start
        # i.e., checking if database connections, API connections, etc. are ready 
        return True
    
    def __poll_resources(self) -> bool:
        if len(self.children) == 0:
            return True
        else:
            for child in self.children:
                queue = child.get_queue()
                signal = queue.get()
            
                if self.signals_received[child.get_name()] is False:
                    self.signals_received[child.get_name()] = True
                    print(f"Received signal '{signal}' from node '{child.get_name()}'")
            
            if all(self.signals_received.values()) is True:
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

    def setup(self) -> None:
        print(f"Setting up node '{self.name}'")
        print(f"Node '{self.name}' setup complete")
    
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
    
    def __run(self) -> None:
        try:
            self.setup()
            # keep in mind that when initializing leaf node, parent nodes may not be initialized yet
            # so some initial signals sent from leaf node may not be received by parent nodes

            while True:
                if self.triggered is False:
                    self.trigger()

                #print(f"Node '{self.name}' triggered")
                if self.__execution() is True:
                    self.__reset_trigger()
                    self.__send_signal()

        except KeyboardInterrupt:
            self.teardown()
            exit(0)
    
    def start(self) -> None:
        print(f"Starting node '{self.name}'")
        proc_listen = Thread(target=self.__run, daemon=True)
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

    node1 = ActionNode("resource1", "preprocess", resource1)
    node2 = ActionNode("resource2", "load_model", resource2)
    node3 = ActionNode("train_model", "train", train_model, listen_to=[node1, node2])

    sorted_nodes = nx.topological_sort(G)
    for node in sorted_nodes:
        node.start()

    time.sleep(5)

    # we can use the to_dict_of_dicts function to output the graph as a json file
    graph = nx.to_dict_of_dicts(G)
    print(graph)