from multiprocessing import Queue, Process, Lock, Value
from typing import List, Any, Dict
import time
import networkx as nx
from logging import Logger
import uuid
from uuid import UUID
import signal

if __name__ == "__main__":
    from constants import Status
else:
    from engine.constants import Status


G = nx.DiGraph()


class BaseNode:

    def __init__(self, 
        name: str, 
        signal_type: str,
        listen_to: List['BaseNode'] = [],
        auto_trigger: bool = False,
    ) -> None:

        self.name = name
        self.signal_type = signal_type
        self.children = listen_to
        self.auto_trigger = auto_trigger

        self.id = uuid.uuid4()
        self.queue = Queue
        self.logger = None
        self.resource_lock = Lock()

        self.triggered = False
        self.signals_received = {child.get_name():Status.WAITING for child in self.children}

        G.add_node(self)

        for child in self.children:
            G.add_edge(child, self, signal_type=self.signal_type)
    
    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name}, uuid: {str(self.get_uuid())})'"

    def get_name(self) -> str:
        return self.name

    def get_queue(self) -> Queue:
        return self.queue

    def get_uuid(self) -> UUID:
        return self.id
    
    def get_resource_lock(self) -> Lock:
        # listening nodes can call this node's get_resource_lock() method inside a context manager to access resources like so:
        # model_loader = ModelLoadingNode()
        # with model_loader.get_resource_lock():
        #     do something with the resource
        return self.resource_lock
    
    def set_logger(self, logger: Logger) -> None:
        # to be called by dag.py to set logger for all nodes
        self.logger = logger
    
    def log(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)
        else:
            print(message)

    def signal_message_template(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "signal_type": self.signal_type,
            "timestamp": time.time(),
            "status": None
        }

    def setup(self) -> None:
        # override to specify actions needed to create node.
        # such actions can include pulling and setting up docker containers, 
        # creating python virtual environments, creating database connections, etc.
        # note that the setup() method will be ran in a separate thread; 
        # this is the main difference between setting up the node using setup() and __init__()
        # therefore, it is best to put set up logic here that is not dependent on other nodes.
        pass

    def precheck(self) -> bool:
        # should be used for continuously checking if the node is ready to start
        # i.e., checking if database connections, API connections, etc. are ready 
        return True

    def pre_execution(self) -> None:
        # override to enable node to do something before execution; 
        # e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        pass

    def execute(self) -> None:
        # the logic for a particular stage in the MLOps pipeline
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
        # override to specify actions to be executed upon removal of node from dag or on pipeline shutdown
        pass
    
    def trigger(self) -> None:
        # method called by user to manually trigger node
        if self.precheck() is True:
            self.triggered = True

    def __send_signal(self, status: Status) -> None:
        self.log(f"Sending signal from node '{self.name}'")
        signal = self.signal_message_template()
        signal["status"] = status
        self.queue.put(signal)
    
    def __execution(self) -> bool:
        try:
            with self.resource_lock:
                self.pre_execution()
                self.execute()
                self.on_success()
            return True
        
        except Exception as e:
            self.log(f"Node '{self.name}' execution failed: {e}")
            self.on_failure()
            return False

    def __reset_trigger(self) -> None:
        self.triggered = False
        if len(self.children) > 0:
            self.signals_received = {child.get_name():Status.WAITING for child in self.children}
    
    def poll_children(self) -> bool:
        if len(self.children) == 0:
            return True
        else:
            for child in self.children:
                queue = child.get_queue()
                signal = self.clear_queue(queue)
            
                if self.signals_received[child.get_name()] == Status.WAITING:
                    self.signals_received[child.get_name()] = signal["status"]
                    self.log(f"Received signal '{signal}' from node '{child.get_name()}'")
            
            if all(value == Status.SUCCESS for value in self.signals_received.values()) is True:
                return True

        return False

    def clear_queue(self, queue: Queue) -> Dict[str, Any]:
        # sleep for a short time to allow queue to be populated
        # time.sleep(0.1)
        # might want to consider making __clear_queue() a public method 
        # because classes that override poll_children() might need to clear the queue
        signal = queue.get()
        while not queue.empty():
            if signal["status"] != Status.SUCCESS:
                queue.get()
            else:
                signal = queue.get()
        return signal

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
                    else:
                        self.__reset_trigger()
                        self.__send_signal(Status.FAILURE)

        except KeyboardInterrupt:
            self.teardown()
            exit(0)

        except Exception as e:
            self.log(f"Node '{self.name}' execution failed: {e}")

    def run(self, run_flag: Value) -> None:
        try:
            self.setup()
        except Exception as e:
            print(f"{str(self)} setup failed")
            return

        while True:
            self.__set_auto_trigger()

            try:
                if run_flag.value == 0:
                    time.sleep(0.5)

                elif run_flag.value == 1:
                    self.__execution()
                
                elif run_flag.value == 2:
                    print(f"ending {str(self)} child process")
                    break

            except Exception as e:
                self.log(f"Node '{self.name}' execution failed: {e}")
            
            except KeyboardInterrupt:
                time.sleep(0.2)


class AndNode(BaseNode):
    def __init__(self, name: str, signal_type: str, listen_to: List[BaseNode] = [], auto_trigger: bool = False) -> None:
        if len(listen_to) < 2:
            raise ValueError("AND node must be listening to more than two nodes")
        super().__init__("AND", "AND", listen_to, auto_trigger)


class ActionNode(BaseNode):
    def __init__(self, name: str, signal_type: str, listen_to: List[BaseNode] = []) -> None:
        super().__init__(name, signal_type, listen_to, auto_trigger=True)


class ResourceNode(BaseNode):
    def __init__(self, name: str, signal_type: str) -> None:
        super().__init__(name, signal_type)


if __name__ == "__main__":
    print("hello")