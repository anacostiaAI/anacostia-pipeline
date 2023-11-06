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
import os

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
        while True:
            with self._status_lock:
                return self._status

    @status.setter
    def status(self, value: Status):
        while True:
            with self._status_lock:
                self._status = value
                break

    def trap_interrupts(self):
        if self.status == Status.PAUSING:
            self.log(f"Node '{self.name}' paused at {datetime.now()}")
            self.status = Status.PAUSED

            # Wait until the node is resumed by the pipeline calling resume()
            while self.status == Status.PAUSED:
                time.sleep(0.1)

        if self.status == Status.EXITING:
            self.log(f"Node '{self.name}' exiting at {datetime.now()}")
            self.on_exit()
            self.log(f"Node '{self.name}' exited at {datetime.now()}")
            self.status = Status.EXITED
            sys.exit(0)

    def log_exception(func):
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

    def check_predecessors_signals(self) -> bool:
        if len(self.predecessors) > 0:
            if self.predecessors_queue.empty():
                return False

            # Pull out all queued up incoming signals and register them
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

    @log_exception
    def setup(self) -> None:
        """
        override to specify actions needed to create node.
        such actions can include pulling and setting up docker containers, 
        creating python virtual environments, creating database connections, etc.
        note that the setup() method will be ran in a separate thread; 
        this is the main difference between setting up the node using setup() and __init__()
        therefore, it is best to the set up logic is not dependent on other nodes.
        """
        pass

    @log_exception
    def on_exit(self):
        """
        on_exit is called when the node is being stopped.
        implement this method to do things like release locks, 
        release resources, anouncing to other nodes that this node has stopped, etc.
        """
        pass


class BaseResourceNode(BaseNode):
    def __init__(self, name: str, resource_path: str, tracker_filename: str, logger: Logger = None) -> None:
        super().__init__(name, [], logger=logger)
        self.resource_path = resource_path
        self.tracker_filename = tracker_filename
        self.iteration = 0
        self.resource_lock = RLock()

    def resource_accessor(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                # this delay is used to allow the json file to be updated before the next iteration
                # in the future, remove this delay and use a thread-safe key-value store (e.g., redis) to store the state of the resource
                time.sleep(0.2)
                with self.resource_lock:
                    return func(self, *args, **kwargs)
        return wrapper
    
    @BaseNode.log_exception
    @resource_accessor
    def start_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    @BaseNode.log_exception
    @resource_accessor
    def record_new(self) -> None:
        """
        override to specify how to detect new files and mark the detected files as 'new'
        """
        raise NotImplementedError
    
    @BaseNode.log_exception
    @resource_accessor
    def record_current(self) -> None:
        """
        override to specify how to label newly created files as 'current'
        """
        pass
    
    @BaseNode.log_exception
    @resource_accessor
    def new_to_current(self) -> None:
        """
        override to specify how to move new files to current
        """
        raise NotImplementedError
    
    @BaseNode.log_exception
    @resource_accessor
    def current_to_old(self) -> None:
        """
        override to specify how to move current files to old
        """
        raise NotImplementedError

    @BaseNode.log_exception
    @resource_accessor
    def trigger_condition(self) -> bool:
        return True

    def run(self) -> None:
        self.start_monitoring()

        while True:
            self.log(f"--------------------------- started iteration {self.iteration} (monitoring phase of {self.name}) at {datetime.now()}")

            # waiting for the trigger condition to be met (note: change status to Work.WAITING_RESOURCE)
            self.trap_interrupts()
            while True:
                self.trap_interrupts()
                try:
                    if self.trigger_condition() is True:
                        break
                    else:
                        self.trap_interrupts()
                        time.sleep(0.1)
                except Exception as e:
                    self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                    continue
                    # Note: we continue here because we want to keep trying to check the resource until it is available
                    # with that said, we should add an option for the user to specify the number of times to try before giving up
                    # and throwing an exception
                    # Note: we also continue because we don't want to stop checking in the case of a corrupted file or something like that. 
                    # We should also think about adding an option for the user to specify what actions to take in the case of an exception,
                    # e.g., send an email to the data science team to let everyone know the resource is corrupted, 
                    # or just not move the file to current.
                
            self.trap_interrupts()
            self.new_to_current()

            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the current state before updating the state of the resource
            # (note: change status to Work.WAITING_SUCCESSORS)
            self.trap_interrupts()
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            
            self.trap_interrupts()
            self.current_to_old()
            self.log(f"--------------------------- finished iteration {self.iteration} (monitoring phase of {self.name}) at {datetime.now()}")

            self.iteration += 1


class BaseActionNode(BaseNode):
    def __init__(self, name: str, predecessors: List[BaseNode], logger: Logger = None) -> None:
        self.iteration = 0
        super().__init__(name, predecessors, logger=logger)

    @BaseNode.log_exception
    def execution_condition(self) -> bool:
        """
        override to specify the condition for executing the action function
        """
        return True

    @BaseNode.log_exception
    def before_execution(self) -> None:
        """
        override to enable node to do something before execution; 
        e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        """
        pass

    @BaseNode.log_exception
    def after_execution(self) -> None:
        """
        override to enable node to do something after executing the action function regardless of the outcome of the action function; 
        e.g., send an email to the data science team to let everyone know the pipeline is done training a new model
        """
        pass

    @BaseNode.log_exception
    def execute(self, *args, **kwargs) -> bool:
        """
        the logic for a particular stage in your MLOps pipeline
        """
        raise NotImplementedError

    @BaseNode.log_exception
    def on_failure(self) -> None:
        """
        override to enable node to do something after execution in event of failure of action_function; 
        e.g., send email to the data science team to let everyone know the pipeline has failed to train a new model
        """
        pass
    
    @BaseNode.log_exception
    def on_error(self, e: Exception) -> None:
        """
        override to enable node to do something after execution in event of error of action_function; 
        e.g., send email to the data science team to let everyone know the pipeline has failed to train a new model due to an error
        """
        pass
    
    @BaseNode.log_exception
    def on_success(self) -> None:
        """
        override to enable node to do something after execution in event of success of action_function; 
        e.g., send an email to the data science team to let everyone know the pipeline has finished training a new model
        """
        pass

    def run(self) -> None:
        while True:
            self.trap_interrupts()
            while self.check_predecessors_signals() is False:
                time.sleep(0.2)
                self.trap_interrupts()
            
            self.log(f"--------------------------- started iteration {self.iteration} (execution phase of {self.name}) at {datetime.now()}")
            self.trap_interrupts()
            self.before_execution()
            ret = False
            try:
                self.trap_interrupts()
                ret = self.execute()
                if ret:
                    self.trap_interrupts()
                    self.on_success()
                else:
                    self.trap_interrupts()
                    self.on_failure()

            except Exception as e:
                self.log(f"Error executing node '{self.name}': {traceback.format_exc()}")
                self.trap_interrupts()
                self.on_error(e)
                return

            self.trap_interrupts()
            self.after_execution()

            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS if ret else Result.FAILURE)

            # checking for successors signals before signalling predecessors will 
            # ensure all action nodes have finished using the current state
            self.trap_interrupts()
            while self.check_successors_signals() is False:
                time.sleep(0.2)
                self.trap_interrupts()

            self.log(f"--------------------------- finished iteration {self.iteration} (execution phase of {self.name}) at {datetime.now()}")

            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS if ret else Result.FAILURE)

            self.iteration += 1
