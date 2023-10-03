from threading import Thread, Lock, Event, RLock
from queue import Queue
from typing import List, Dict, Optional, Set, Union
import time
from logging import Logger
from datetime import datetime
from functools import wraps
import traceback
import sys
from pydantic import BaseModel

if __name__ == "__main__":
    from constants import Status
else:
    from engine.constants import Status


class Message(BaseModel):
    sender: str
    timestamp: datetime
    status: Status = None


class BaseNode(Thread):
    def __init__(self, name: str, predecessors: List['BaseNode'], successors: List['BaseNode']) -> None:
        self.name = name
        self.successors = successors
        self.predecessors = predecessors
        self._status_lock = Lock()
        self._status = Status.OFF
        self.predecessors_queue = Queue()
        self.successors_queue = Queue()
        
        self.received_predecessors_signals: Dict[str, Message] = dict()
        self.received_successors_signals: Dict[str, Message] = dict()

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name}, status: {str(self.status)})'"
    
    def set_logger(self, logger: Logger) -> None:
        self.logger = logger

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
    def pausable(func):
        '''
        A Decorator for allowing execution in the Status.RUNNING state to be paused mid execution
        '''
        def wrapper(self, *args, **kwargs):
            while self.status == Status.PAUSED:
                time.sleep(0.1)

            ret = func(self, *args, **kwargs)
            return ret
        return wrapper

    def user_defined(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try: 
                ret = func(self, *args, **kwargs)
                return ret
            except Exception as e:
                print(f"Error in user-defined method '{func.__name__}' of node '{self.name}': {traceback.format_exc()}")
                # self.status = Status.ERROR
                return
        return wrapper
    
    def signal_successors(self, status:Status):
        msg = Message(
            sender = self.name,
            timestamp = datetime.now(),
            status = status
        )

        for successor in self.successors:
            successor.successors_queue.put(msg)

    def signal_predecessors(self, status:Status):
        msg = Message(
            sender = self.name,
            timestamp = datetime.now(),
            status = status
        )

        for predecessors in self.predecessors:
            predecessors.predecessors_queue.put(msg)

    def check_predecessors_signals(self) -> bool:
        if len(self.predecessors) > 0:
            if self.predecessors_queue.empty():
                return False

            # Pull out the queued up incoming signals and register them
            while not self.predecessors_queue.empty():
                sig: Message = self.predecessors_queue.get()

                if sig.sender not in self.received_predecessors_signals:
                    self.received_predecessors_signals[sig.sender] = sig.status
                else:
                    if self.received_predecessors_signals[sig.sender] != Status.SUCCESS:
                        self.received_predecessors_signals[sig.sender] = sig.status
                # TODO For signaling over the network, this is where we'd send back an ACK

            # Check if the signals match the execute condition
            if len(self.received_predecessors_signals) == len(self.predecessors):
                if all([sig == Status.SUCCESS for sig in self.received_predecessors_signals.values()]):
                    return True
                else:
                    return False
            else:
                return False
        
        # If there are no dependent nodes, then we can just return True
        return True
    
    def check_successors_signals(self) -> bool:
        if len(self.successors) > 0:
            if self.successors_queue.empty():
                return False

            # Pull out the queued up incoming signals and register them
            while not self.successors_queue.empty():
                sig: Message = self.successors_queue.get()

                if sig.sender not in self.received_successors_signals:
                    self.received_successors_signals[sig.sender] = sig.status
                else:
                    if self.received_successors_signals[sig.sender] != Status.SUCCESS:
                        self.received_successors_signals[sig.sender] = sig.status
                # TODO For signaling over the network, this is where we'd send back an ACK

            # Check if the signals match the execute condition
            if len(self.received_successors_signals) == len(self.successors):
                if all([sig == Status.SUCCESS for sig in self.received_successors_signals.values()]):
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

    def stop(self):
        self.status = Status.EXITING

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

    def on_exit(self):
        """
        on_exit is called when the node is being stopped.
        implement this method to do things like release locks, 
        release resources, anouncing to other nodes that this node has stopped, etc.
        """
        pass

    def teardown(self) -> None:
        """
        override to specify actions to be executed upon removal of node from dag or on pipeline shutdown
        """
        pass


class BaseResourceNode(BaseNode):
    def __init__(
        self, name: str, 
        successors: List['BaseActionNode'],
        auto_trigger: bool = False
    ) -> None:
        self.auto_trigger = auto_trigger
        self.triggered = auto_trigger
        self.resource_lock = RLock()
        super().__init__(name, [], successors)

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

    @resource_accessor
    def update_state(self):
        pass

    @resource_accessor
    def check_resource(self) -> bool:
        return True

    @resource_accessor
    def trigger_condition(self) -> bool:
        """
        in the default implementation, we trigger the next node as soon as we see a new artifact.
        override this method to specify a more complex trigger condition (e.g., a cron schedule or a condition).
        """
        return True
    
    def trigger(self) -> None:
        self.triggered = True

    def reset_trigger(self):
        # TODO reset trigger dependent on the state of the system i.e. data store, feature store, model store
        if self.auto_trigger == False:
            self.triggered = False

    def run(self) -> None:
        # --------------------------- iteration 0 (initialization) ---------------------------
        # setting up the node and the resource
        self.status = Status.INIT
        try:
            self.setup()
        except Exception as e:
            print(f"Error setting up node '{self.name}': {traceback.format_exc()}")
            self.status = Status.ERROR
            return
    
        # waiting for the trigger condition to be met
        self.status = Status.WAITING_RESOURCE
        while True:
            try:
                if self.check_resource() is True:
                    break
            except Exception as e:
                print(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                continue
                # Note: we continue here because we want to keep trying to check the resource until it is available
                # with that said, we should add an option for the user to specify the number of times to try before giving up
                # and throwing an exception
        
        # updating the state of the resource in case the trigger condition is met in iteration 0
        self.status = Status.UPDATE_STATE
        try:
            self.update_state()
        except Exception as e:
            print(f"Error updating state in node '{self.name}': {traceback.format_exc()}")
            self.status = Status.ERROR
            return

        self.signal_successors(Status.SUCCESS)
        # --------------------------- end of iteration 0 (initialization phase) ---------------------------

        # --------------------------- iteration 1+ (monitoring phase) -------------------------------------
        while True:

            def trap():
                if self.status == Status.PAUSING:
                    self.log(f"Node {self.name} paused")
                    self.status = Status.PAUSED

                    while self.status == Status.PAUSED:
                        time.sleep(0.1)
                
                elif self.status == Status.EXITING:
                    self.log(f"Node {self.name} exiting")
                    self.teardown()
                    self.status = Status.EXITED
                    self.log(f"Node {self.name} exited")
                    break
                
            trap()

            # check the resource to see if the trigger condition is met, and if so, signal the next node
            self.status = Status.WAITING_RESOURCE
            try:
                resource_check = self.check_resource()
                trap()
            except Exception as e:
                # Note: the only function that should throw an exception is check_resource() because it is a user-defined function
                # the functions that we've created should not throw exceptions
                print(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                resource_check = False

            trap()

            # signal the successors to execute if the trigger condition is met
            self.signal_successors(Status.SUCCESS if resource_check is True else Status.FAILURE)

            trap()

            # check for successors signals before updating state to ensure all successors have finished using the current state
            self.status = Status.WAITING_SUCCESSORS
            if self.check_successors_signals() is False:
                continue

            trap()

            # if all successors have finished using the state, then update the state of the resource
            self.status = Status.UPDATE_STATE
            try:
                self.update_state()
            except Exception as e:
                print(f"Error updating state in node '{self.name}': {traceback.format_exc()}")
                self.status = Status.ERROR
                return
            
            trap()
        # --------------------------- iteration 1+ (monitoring phase) ---------------------------------------


class BaseActionNode(BaseNode):
    def __init__(
        self, 
        name: str,
        predecessors: List[BaseNode], 
        successors: List['BaseActionNode'],
        auto_trigger: bool = False
    ) -> None:
        self.auto_trigger = auto_trigger
        self.triggered = auto_trigger
        super().__init__(name, predecessors, successors)

    @BaseNode.pausable
    def on_failure(self, e: Exception = None) -> None:
        """
        override to enable node to do something after execution in event of failure of action_function; 
        e.g., send an email to the data science team to let everyone know the pipeline has failed to train a new model
        """
        pass
    
    @BaseNode.pausable
    def pre_execution(self) -> None:
        """
        override to enable node to do something before execution; 
        e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        """
        pass

    @BaseNode.pausable
    def execute(self, *args, **kwargs) -> bool:
        """
        the logic for a particular stage in your MLOps pipeline
        """
        return True

    @BaseNode.pausable
    def post_execution(self) -> None:
        """
        override to enable node to do something after execution; 
        e.g., send an email to the data science team to let everyone know the pipeline is has trained a new model
        """
        pass
    
    @BaseNode.pausable
    def on_success(self) -> None:
        """
        override to enable node to do something after execution in event of success of action_function; 
        e.g., send an email to the data science team to let everyone know the pipeline has finished training a new model
        """
        pass

    def trigger(self) -> None:
        self.triggered = True

    def reset_trigger(self):
        # TODO reset trigger dependent on the state of the system i.e. data store, feature store, model store
        if self.auto_trigger == False:
            self.triggered = False

    def run(self) -> None:
        self.status = Status.INIT
        try:
            self.setup()
        except Exception as e:
            print(f"Error setting up node '{self.name}': {traceback.format_exc()}")
            self.status = Status.ERROR
            return

        while True:
            if self.check_predecessors_signals() is False:
                self.status = Status.WAITING
                continue
            else:
                self.status = Status.RUNNING

            if self.status == Status.RUNNING:               

                if self.triggered:
                    self.pre_execution()

                    try:
                        ret = self.execute()
                        if ret:
                            self.on_success()
                            self.post_execution()
                            self.signal_successors(Status.SUCCESS)
                            self.signal_predecessors(Status.SUCCESS)

                        else:
                            self.on_failure()
                            self.post_execution()
                            self.signal_successors(Status.FAILURE)
                            self.signal_predecessors(Status.FAILURE)

                    except Exception as e:
                        print(f"Error in execution method of node '{self.name}': {traceback.format_exc()}")
                        # what happens if the user-defined on_failure method also throws an exception?
                        # TODO: add a try-except block around the on_failure method
                        self.on_failure(e)
                        self.post_execution()
                        self.signal_successors(Status.FAILURE)
                        self.signal_predecessors(Status.FAILURE)

                    self.reset_trigger()    
                    time.sleep(0.1)

            elif self.status == Status.PAUSED:
                # Stay Indefinitely Paused until external action
                time.sleep(0.1)

            elif self.status == Status.STOPPING:
                # TODO: what happens if the user-defined on_exit method throws an exception?
                # TODO: add a try-except block around the on_exit method and change status to Status.EXITED if exception is thrown
                self.on_exit()
                self.status = Status.EXITED
                return
            
            time.sleep(0.1)
