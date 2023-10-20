from threading import Thread, Lock, RLock
from queue import Queue
from typing import List, Dict
import time
from logging import Logger
from datetime import datetime
from functools import wraps
import traceback
import sys
from pydantic import BaseModel

if __name__ == "__main__":
    from constants import Status, Result, Work
else:
    from engine.constants import Status, Result, Work


class Message(BaseModel):
    sender: str
    timestamp: datetime
    result: Result = None


class BaseNode(Thread):
    def __init__(self, name: str, predecessors: List['BaseNode'], logger: Logger = None) -> None:
        self._status_lock = Lock()
        self._status = Status.OFF
        self.work_list = set()
        self.logger = logger
        self.anacostia_path: str = None

        self.successors: List['BaseNode'] = list()
        self.predecessors = predecessors
        self.predecessors_queue = Queue()
        self.successors_queue = Queue()
        self.received_predecessors_signals: Dict[str, Message] = dict()
        self.received_successors_signals: Dict[str, Message] = dict()
        super().__init__(name=name)
    
    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name})'"
    
    def set_anacostia_path(self, path: str) -> None:
        self.anacostia_path = path
    
    def set_logger(self, logger: Logger) -> None:
        if self.logger is None:
            self.logger = logger
        else:
            raise ValueError(f"Logger for node '{self.name}' has already been set.")

    def log(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)
        else:
            print(message)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value: Status):
        while True:
            with self._status_lock:
                self._status = value
                break

    @staticmethod
    def trap_interrupts(func):
        """
        A Decorator for allowing the node to be paused mid execution
        """
        def wrapper(self, *args, **kwargs):
            if self.status == Status.PAUSING:
                self.log(f"Node {self.name} paused at {datetime.now()}")
                self.status = Status.PAUSED

                # Wait until the node is resumed by the pipeline calling resume()
                while self.status == Status.PAUSED:
                    time.sleep(0.1)

            if self.status == Status.EXITING:
                self.log(f"Node {self.name} exiting at {datetime.now()}")
                self.on_exit()
                self.log(f"Node {self.name} exited at {datetime.now()}")
                self.status = Status.EXITED
                sys.exit(0)

            ret = func(self, *args, **kwargs)
            return ret
        return wrapper

    def trap_exceptions(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try: 
                ret = func(self, *args, **kwargs)
                return ret
            except Exception as e:
                self.log(f"Error in user-defined method '{func.__name__}' of node '{self.name}': {traceback.format_exc()}")
                self.status = Status.ERROR
                return
        return wrapper
    
    def signal_successors(self, result: Result):
        msg = Message(
            sender = self.name,
            timestamp = datetime.now(),
            result = result
        )
        for successor in self.successors:
            successor.predecessors_queue.put(msg)

    def signal_predecessors(self, result: Result):
        msg = Message(
            sender = self.name,
            timestamp = datetime.now(),
            result = result
        )
        for predecessor in self.predecessors:
            predecessor.successors_queue.put(msg)

    @trap_interrupts
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
                if all([sig == Result.SUCCESS for sig in self.received_predecessors_signals.values()]):

                    # Reset the received signals
                    self.received_predecessors_signals = dict()
                    return True
                else:
                    return False
            else:
                return False
 
        # If there are no dependent nodes, then we can just return True
        return True
    
    @trap_interrupts
    def check_successors_signals(self) -> bool:
        if len(self.successors) > 0:
            if self.successors_queue.empty():
                return False

            # Pull out the queued up incoming signals and register them
            while not self.successors_queue.empty():
                sig: Message = self.successors_queue.get()

                if sig.sender not in self.received_successors_signals:
                    self.received_successors_signals[sig.sender] = sig.result
                else:
                    if self.received_successors_signals[sig.sender] != Result.SUCCESS:
                        self.received_successors_signals[sig.sender] = sig.result
                # TODO For signaling over the network, this is where we'd send back an ACK

            # Check if the signals match the execute condition
            if len(self.received_successors_signals) == len(self.successors):
                if all([sig == Result.SUCCESS for sig in self.received_successors_signals.values()]):

                    # Reset the received signals
                    self.received_successors_signals = dict()
                    return True
                else:
                    return False
            else:
                return False
        
        # If there are no dependent nodes, then we can just return True
        return True

    def pause(self):
        self.status = Status.PAUSING

    def resume(self):
        self.status = Status.RUNNING

    def exit(self):
        self.status = Status.EXITING

    @trap_exceptions
    def setup(self) -> None:
        """
        override to specify actions needed to create node.
        such actions can include pulling and setting up docker containers, 
        creating python virtual environments, creating database connections, etc.
        note that the setup() method will be ran in a separate thread; 
        this is the main difference between setting up the node using setup() and __init__()
        therefore, it is best to put set up logic here that is not dependent on other nodes.
        """
        pass

    @trap_exceptions
    def on_exit(self):
        """
        on_exit is called when the node is being stopped.
        implement this method to do things like release locks, 
        release resources, anouncing to other nodes that this node has stopped, etc.
        """
        pass


class BaseResourceNode(BaseNode):
    def __init__(
        self, 
        name: str, 
        uri: str,
        logger: Logger = None
    ) -> None:
        self.uri = uri
        self.iteration = 0
        self.resource_lock = RLock()
        super().__init__(name, [], logger=logger)

    def resource_accessor(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # make sure setup is finished before allowing other nodes to access the resource
            # Note: this means any function decorated with the resource_accessor cannot be called inside the setup method
            while self.status == Status.INIT:
                if func.__name__ == "setup":
                    break
                else:
                    time.sleep(0.1)

            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                with self.resource_lock:
                    return func(self, *args, **kwargs)
        return wrapper

    @BaseNode.trap_interrupts
    @BaseNode.trap_exceptions
    @resource_accessor
    def update_state(self) -> None:
        """
        override to specify how the state of the resource is updated
        """
        raise NotImplementedError

    @BaseNode.trap_interrupts
    @resource_accessor
    def check_resource(self) -> bool:
        return True

    def run(self) -> None:
        self.log(f"--------------------------- started iteration {self.iteration} (initialization phase of {self.name}) at {datetime.now()}")
        self.status = Status.INIT
        self.setup()
        self.status = Status.RUNNING
    
        # waiting for the trigger condition to be met
        while True:
            try:
                if self.check_resource() is True:
                    break
                else:
                    time.sleep(0.1)
            except Exception as e:
                print(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                continue
                # Note: we continue here because we want to keep trying to check the resource until it is available
                # with that said, we should add an option for the user to specify the number of times to try before giving up
                # and throwing an exception
        
        # updating the state of the resource in case the trigger condition is met in iteration 0
        self.update_state()

        self.log(f"--------------------------- finished iteration {self.iteration} (initialization phase of {self.name}) at {datetime.now()}")
        self.iteration += 1
        time.sleep(0.2)
        self.signal_successors(Result.SUCCESS)

        self.log(f"--------------------------- started iteration {self.iteration} (monitoring phase of {self.name}) at {datetime.now()}")
        while True:
            # check the resource to see if the trigger condition is met, and if so, signal the next node
            resource_check = False
            try:
                resource_check = self.check_resource()
            except Exception as e:
                # Note: the only function that should throw an exception is check_resource() because it is a user-defined function
                # the functions that we've created should not throw exceptions
                print(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")

            # check for successors signals before updating state to ensure all successors have finished using the current state
            if self.check_successors_signals() is True:
                #self.log(f"--------------------------- finished iteration {self.iteration} (monitoring phase of {self.name}) at {datetime.now()}")

                # if all successors have finished using the state, then update the state of the resource
                self.update_state()
                self.iteration += 1
                #self.log(f"--------------------------- started iteration {self.iteration} (monitoring phase of {self.name}) at {datetime.now()}")

                # signal the successors to execute if the trigger condition is met
                self.signal_successors(Result.SUCCESS if resource_check else Result.FAILURE)


class BaseActionNode(BaseNode):
    def __init__(
        self, 
        name: str,
        predecessors: List[BaseNode], 
        logger: Logger = None
    ) -> None:
        self.iteration = 0
        super().__init__(name, predecessors, logger=logger)

    @BaseNode.trap_interrupts
    @BaseNode.trap_exceptions
    def before_execution(self) -> None:
        """
        override to enable node to do something before execution; 
        e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        """
        pass

    @BaseNode.trap_interrupts
    @BaseNode.trap_exceptions
    def after_execution(self) -> None:
        """
        override to enable node to do something after executing the action function regardless of the outcome of the action function; 
        e.g., send an email to the data science team to let everyone know the pipeline is done training a new model
        """
        pass

    @BaseNode.trap_interrupts
    def execute(self, *args, **kwargs) -> bool:
        """
        the logic for a particular stage in your MLOps pipeline
        """
        raise NotImplementedError

    @BaseNode.trap_interrupts
    @BaseNode.trap_exceptions
    def on_failure(self) -> None:
        """
        override to enable node to do something after execution in event of failure of action_function; 
        e.g., send email to the data science team to let everyone know the pipeline has failed to train a new model
        """
        pass
    
    @BaseNode.trap_interrupts
    @BaseNode.trap_exceptions
    def on_error(self, e: Exception) -> None:
        """
        override to enable node to do something after execution in event of error of action_function; 
        e.g., send email to the data science team to let everyone know the pipeline has failed to train a new model due to an error
        """
        pass
    
    @BaseNode.trap_interrupts
    @BaseNode.trap_exceptions
    def on_success(self) -> None:
        """
        override to enable node to do something after execution in event of success of action_function; 
        e.g., send an email to the data science team to let everyone know the pipeline has finished training a new model
        """
        pass

    def run(self) -> None:
        self.log(f"--------------------------- started iteration 0 (initialization phase of {self.name}) at {datetime.now()}")
        self.status = Status.INIT
        self.setup()
        self.status = Status.RUNNING
        self.log(f"--------------------------- finished iteration 0 (initialization phase of {self.name}) at {datetime.now()}")
        self.iteration += 1

        while True:
            time.sleep(0.2)
            while self.check_predecessors_signals() is False:
                time.sleep(0.1)
            
            self.log(f"--------------------------- started iteration {self.iteration} (execution phase of {self.name}) at {datetime.now()}")
            self.before_execution()
            ret = False
            try:
                ret = self.execute()
                if ret:
                    self.on_success()
                else:
                    self.on_failure()

            except Exception as e:
                print(f"Error executing node '{self.name}': {traceback.format_exc()}")
                self.on_error(e)
                return

            self.after_execution()
            self.log(f"--------------------------- finished iteration {self.iteration} (execution phase of {self.name}) at {datetime.now()}")

            time.sleep(0.2)
            self.signal_successors(Result.SUCCESS if ret else Result.FAILURE)

            # checking for successors signals before signalling predecessors will 
            # ensure all action nodes have finished using the current state
            while self.check_successors_signals() is False:
                time.sleep(0.1)

            time.sleep(0.2)
            self.signal_predecessors(Result.SUCCESS if ret else Result.FAILURE)
            self.iteration += 1
