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
    receiver: str
    timestamp: datetime
    result: Result = None

    def __repr__(self) -> str:
        return f"Message(sender: {self.sender}, receiver: {self.receiver}, timestamp: {str(self.timestamp)}, result: {self.result})"


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
        for successor in self.successors:
            msg = Message(
                sender = self.name,
                receiver = successor.name,
                timestamp = datetime.now(),
                result = result
            )
            self.log(f"'{self.name}' signaled successor '{successor.name}'")
            successor.predecessors_queue.put(msg)

    def signal_predecessors(self, result: Result):
        for predecessor in self.predecessors:
            msg = Message(
                sender = self.name,
                receiver = predecessor.name,
                timestamp = datetime.now(),
                result = result
            )
            self.log(f"'{self.name}' signaled predecessor '{predecessor.name}'")
            predecessor.successors_queue.put(msg)

    def check_predecessors_signals(self) -> bool:
        # If there are no predecessors, then we can just return True
        if len(self.predecessors) == 0:
            return True

        # If there are predecessors, but no signals, then we can return False
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
 
    def check_successors_signals(self) -> bool:
        # If there are no successors, then we can just return True
        if len(self.successors) == 0:
            return True

        # If there are successors, but no signals, then we can return False
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

    def run(self) -> None:
        """
        override to specify the logic of the node.
        """
        raise NotImplementedError


class BaseMetadataStoreNode(BaseNode):
    """
    Base class for metadata store nodes.
    Metadata store nodes are nodes that are used to store metadata about the pipeline; 
    e.g., store information about experiments (start time, end time, metrics, etc.).
    The metadata store node is a special type of resource node that will be the predecessor of all other resource nodes;
    thus, by extension, the metadata store node will always be the root node of the DAG.
    """
    def __init__(
        self, name: str, tracker_filename: str, logger: Logger = None) -> None:
        super().__init__(name, predecessors=[], logger=logger)
        self.tracker_filename = tracker_filename
        self.run_id = 0
        self.resource_lock = RLock()
    
    def get_run_id(self) -> int:
        return self.run_id

    def metadata_accessor(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                # this delay is used to allow the json file to be updated before the next iteration
                # in the future, remove this delay and use a thread-safe key-value store (e.g., redis) to store the state of the resource
                with self.resource_lock:
                    result = func(self, *args, **kwargs)
                    time.sleep(0.2)
                    return result
        return wrapper
    
    @metadata_accessor
    def create_run(self) -> None:
        """
        Override to specify how to create a run in the metadata store.
        E.g., log the start time of the run, log the parameters of the run, etc. 
        or create a new run in MLFlow, create a new run in Neptune, etc.
        """
        raise NotImplementedError
    
    @metadata_accessor
    def end_run(self) -> None:
        """
        Override to specify how to end a run in the metadata store.
        E.g., log the end time of the run, log the metrics of the run, update run_number, etc.
        or end a run in MLFlow, end a run in Neptune, etc.
        """
        self.run_id += 1
    
    def run(self) -> None:
        while True:
            # waiting for all resource nodes to signal their resources are ready to be used
            self.trap_interrupts()
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)

            # creating a new run
            self.trap_interrupts()
            self.create_run()

            # signal to all successors that the run has been created; i.e., begin pipeline execution
            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)

            # waiting for all resource nodes to signal they are done using the current state
            self.trap_interrupts()
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            
            # ending the run
            self.trap_interrupts()
            self.end_run()


class BaseResourceNode(BaseNode):
    def __init__(
        self, 
        name: str, resource_path: str, tracker_filename: str, metadata_store: BaseMetadataStoreNode,
        logger: Logger = None, monitoring: bool = True
    ) -> None:
        super().__init__(name, predecessors=[metadata_store], logger=logger)
        self.resource_path = resource_path
        self.tracker_filename = tracker_filename
        self.resource_lock = RLock()
        self.monitoring = monitoring
        self.metadata_store = metadata_store

    def resource_accessor(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                # this delay is used to allow the json file to be updated before the next iteration
                # in the future, remove this delay and use a thread-safe key-value store (e.g., redis) to store the state of the resource
                with self.resource_lock:
                    result = func(self, *args, **kwargs)
                    time.sleep(0.2)
                    return result
        return wrapper
    
    def monitoring_enabled(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.monitoring is True:
                return func(self, *args, **kwargs)
            else:
                # TODO: change this ValueError to a custom exception
                raise ValueError(f"Cannot call '{func.__name__}' when monitoring is disabled for node 'self.name'.")
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
    def stop_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    def on_exit(self):
        if self.monitoring is True:
            self.stop_monitoring()

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
        # if the node is not monitoring the resource, then we don't need to start the observer / monitoring thread
        if self.monitoring is True:
            self.start_monitoring()

        while True:
            # if the node is not monitoring the resource, then we don't need to check for new files
            if self.monitoring is True:
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
                
            # signal to metadata store node that the resource is ready to be used; i.e., the resource is ready to be used for the next run
            # (note: change status to Work.READY)
            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS)

            # wait for metadata store node to finish creating the run before moving files to current
            self.trap_interrupts()
            while self.check_predecessors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)

            # if the node is not monitoring the resource, then there will not be any new files to move to current
            if self.monitoring is True:
                self.trap_interrupts()
                self.new_to_current()

            # signalling to all successors that the resource is ready to be used
            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the current state before updating the state of the resource
            # (note: change status to Work.WAITING_SUCCESSORS)
            self.trap_interrupts()
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            
            # moving 'current' files to 'old'
            self.trap_interrupts()
            self.current_to_old()

            # signal the metadata store node that the resource has been used for the current run
            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS)


class BaseActionNode(BaseNode):
    def __init__(self, name: str, predecessors: List[BaseNode], logger: Logger = None) -> None:
        self.iteration = 0
        super().__init__(name, predecessors, logger=logger)

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
            
            self.trap_interrupts()
            self.before_execution()
            ret = None
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

            finally:
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

            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS if ret else Result.FAILURE)

            self.iteration += 1
