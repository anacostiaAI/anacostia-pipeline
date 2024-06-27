from __future__ import annotations
import os
from threading import Thread, Lock, RLock
from typing import List, Union, Optional
import time
from logging import Logger
from datetime import datetime
from functools import wraps
import traceback
import sys
from pydantic import BaseModel, ConfigDict

from .constants import Status, Result, Work
from .utils import Signal, SignalTable
from ..dashboard.subapps.basenode import BaseNodeApp



class NodeModel(BaseModel):
    '''
    A Pydantic Model for validation and serialization of a BaseNode
    '''
    model_config = ConfigDict(from_attributes=True)

    name: str
    type: Optional[str]
    predecessors: List[str]
    successors: List[str]



class BaseNode(Thread):
    def __init__(
        self, name: str, predecessors: List[BaseNode] = None, 
        loggers: Union[Logger, List[Logger]] = None, endpoint: str = None
    ) -> None:
        self._status_lock = Lock()
        self._status = Status.OFF
        self.work_list = list()
        
        if loggers is None:
            self.loggers: List[Logger] = list()
        else:
            if isinstance(loggers, Logger):
                self.loggers: List[Logger] = [loggers]
            else:
                self.loggers: List[Logger] = loggers
        
        # TODO: replace list with tuple
        if predecessors is None:
            predecessors = list()
        
        self.predecessors = predecessors
        self.predecessors_signals = SignalTable()
        self.successors: List[BaseNode] = list()
        self.successors_signals = SignalTable()

        super().__init__(name=name)
    
    def get_app(self):
        return BaseNodeApp(self)

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name})'"
    
    def model(self):
        return NodeModel(
            name = self.name,
            type = type(self).__name__,
            predecessors = [n.name for n in self.predecessors],
            successors = [n.name for n in self.successors]
        )

    def add_loggers(self, loggers: Union[Logger, List[Logger]]) -> None:
        if isinstance(loggers, Logger):
            self.loggers.append(loggers)
        else:
            self.loggers.extend(loggers)

    def log(self, message: str, level="DEBUG") -> None:
        if len(self.loggers) > 0:
            for logger in self.loggers:
                if level == "DEBUG":
                    logger.debug(message)
                elif level == "INFO":
                    logger.info(message)
                elif level == "WARNING":
                    logger.warning(message)
                elif level == "ERROR":
                    logger.error(message)
                elif level == "CRITICAL":
                    logger.critical(message)
                else:
                    raise ValueError(f"Invalid log level: {level}")
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
        def log_exception_wrapper(self, *args, **kwargs):
            try: 
                ret = func(self, *args, **kwargs)
                return ret
            except Exception as e:
                self.log(f"Error in user-defined method '{func.__name__}' of node '{self.name}': {traceback.format_exc()}")
                self.status = Status.ERROR
                return
        return log_exception_wrapper
    
    def signal_successors(self, result: Result):
        for successor in self.successors:
            msg = Signal(
                sender = self.name,
                receiver = successor.name,
                timestamp = datetime.now(),
                result = result
            )
            if successor.name not in self.successors_signals.keys():
                successor.predecessors_signals[self.name] = msg
            else:
                if self.successors_signals[successor.name].result != Result.SUCCESS:
                    successor.predecessors_signals[self.name] = msg

    def signal_predecessors(self, result: Result):
        for predecessor in self.predecessors:
            msg = Signal(
                sender = self.name,
                receiver = predecessor.name,
                timestamp = datetime.now(),
                result = result
            )
            if predecessor.name not in self.predecessors_signals.keys():
                predecessor.successors_signals[self.name] = msg
            else:
                if self.predecessors_signals[predecessor.name].result != Result.SUCCESS:
                    predecessor.successors_signals[self.name] = msg

    def check_predecessors_signals(self) -> bool:
        # If there are no predecessors, then we can just return True
        if len(self.predecessors) == 0:
            return True

        # If there are predecessors, but no signals, then we can return False
        if len(self.predecessors_signals) == 0:
            return False

        # Check if all the predecessors have sent a signal
        if len(self.predecessors_signals) == len(self.predecessors):

            # Check if all predecessors have sent a success signal
            if all([sig.result == Result.SUCCESS for sig in self.predecessors_signals.values()]):

                # Reset the received signals
                self.predecessors_signals = SignalTable()
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
        if len(self.successors_signals) == 0:
            return False

        # Check if the signals match the execute condition
        if len(self.successors_signals) == len(self.successors):
            if all([sig.result == Result.SUCCESS for sig in self.successors_signals.values()]):

                # Reset the received signals
                self.successors_signals = SignalTable()
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
        self.status = Status.INIT

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
        self,
        name: str,
        uri: str,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
    
        super().__init__(name, predecessors=[], loggers=loggers)
        self.uri = uri
        self.run_id = 0
        self.resource_lock = RLock()
    
    def resource_uri(self, r_node: BaseResourceNode):
        raise NotImplementedError

    def get_run_id(self) -> int:
        return self.run_id

    def metadata_accessor(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                with self.resource_lock:
                    result = func(self, *args, **kwargs)
                    return result
        return wrapper
    
    @metadata_accessor
    def create_resource_tracker(self, resource_node: 'BaseResourceNode') -> None:
        raise NotImplementedError
    
    @metadata_accessor
    def create_entry(self, resource_node: 'BaseResourceNode', **kwargs) -> None:
        raise NotImplementedError

    @metadata_accessor
    def get_entries(self, resource_node: 'BaseResourceNode') -> List[dict]:
        pass

    @metadata_accessor
    def update_entry(self, resource_node: 'BaseResourceNode', entry_id: int, **kwargs) -> None:
        pass

    @metadata_accessor
    def get_num_entries(self, resource_node: 'BaseResourceNode') -> int:
        pass

    @metadata_accessor
    def log_metrics(self, **kwargs) -> None:
        pass
    
    @metadata_accessor
    def log_params(self, **kwargs) -> None:
        pass

    @metadata_accessor
    def set_tags(self, **kwargs) -> None:
        pass

    @metadata_accessor
    def start_run(self) -> None:
        """
        Override to specify how to create a run in the metadata store.
        E.g., log the start time of the run, log the parameters of the run, etc. 
        or create a new run in MLFlow, create a new run in Neptune, etc.
        """
        raise NotImplementedError

    @metadata_accessor
    def add_run_id(self) -> None:
        """
        Override to specify how to add the run_id to the resource metadata in the metadata store.
        """
        raise NotImplementedError
    
    @metadata_accessor
    def add_end_time(self) -> None:
        """
        Override to specify how to add the end_time to the resource metadata in the metadata store.
        """
        raise NotImplementedError
    
    @metadata_accessor
    def end_run(self) -> None:
        """
        Override to specify how to end a run in the metadata store.
        E.g., log the end time of the run, log the metrics of the run, update run_number, etc.
        or end a run in MLFlow, end a run in Neptune, etc.
        """
        raise NotImplementedError
    
    def run(self) -> None:
        while True:
            # waiting for all resource nodes to signal their resources are ready to be used
            self.trap_interrupts()
            self.work_list.append(Work.WAITING_SUCCESSORS)
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            self.work_list.remove(Work.WAITING_SUCCESSORS)
            # note: since the UI polls the work_list every 500ms, the UI will always display WAITING_SUCCESSORS 
            # because it doesn't (and possibly can never) poll fast enough to catch the work_list without WAITING_SUCCESSORS

            # creating a new run
            self.trap_interrupts()
            self.work_list.append(Work.STARTING_RUN)
            self.start_run()
            self.add_run_id()
            self.work_list.remove(Work.STARTING_RUN)

            # signal to all successors that the run has been created; i.e., begin pipeline execution
            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)

            # waiting for all resource nodes to signal they are done using the current state
            self.trap_interrupts()
            self.work_list.append(Work.WAITING_SUCCESSORS)
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            self.work_list.remove(Work.WAITING_SUCCESSORS)
            
            # ending the run
            self.trap_interrupts()
            self.work_list.append(Work.ENDING_RUN)
            self.add_end_time()
            self.end_run()
            self.work_list.remove(Work.ENDING_RUN)

            self.run_id += 1
            
            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)


class BaseResourceNode(BaseNode):
    def __init__(
        self, 
        name: str, resource_path: str, metadata_store: BaseMetadataStoreNode,
        loggers: Union[Logger, List[Logger]] = None, monitoring: bool = True
    ) -> None:
        super().__init__(name, predecessors=[metadata_store], loggers=loggers)
        self.resource_path = resource_path
        self.resource_lock = RLock()
        self.monitoring = monitoring
        self.metadata_store = metadata_store

    def resource_accessor(func):
        @wraps(func)
        def resource_accessor_wrapper(self, *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                with self.resource_lock:
                    result = func(self, *args, **kwargs)
                    return result
        return resource_accessor_wrapper
    
    # consider removing this decorator in the future. it hasn't been used for anything.
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
    def start_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    @BaseNode.log_exception
    def stop_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    def on_exit(self):
        if self.monitoring is True:
            self.stop_monitoring()
            self.work_list.remove(Work.MONITORING_RESOURCE)

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
    def trigger_condition(self) -> bool:
        return True

    def run(self) -> None:
        # if the node is not monitoring the resource, then we don't need to start the observer / monitoring thread
        if self.monitoring is True:
            self.work_list.append(Work.MONITORING_RESOURCE)
            self.start_monitoring()

        while True:
            # if the node is not monitoring the resource, then we don't need to check for new files
            if self.monitoring is True:
                self.trap_interrupts()
                self.work_list.append(Work.WAITING_RESOURCE)
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
                self.work_list.remove(Work.WAITING_RESOURCE)
                
            # signal to metadata store node that the resource is ready to be used for the next run
            # i.e., tell the metadata store to create and start the next run
            # e.g., there is enough new data to trigger the next run
            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS)

            # wait for metadata store node to finish creating the run 
            self.trap_interrupts()
            self.work_list.append(Work.WAITING_PREDECESSORS)
            while self.check_predecessors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            self.work_list.remove(Work.WAITING_PREDECESSORS)

            # signalling to all successors that the resource is ready to be used for the current run
            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the the resource for the current run
            self.trap_interrupts()
            self.work_list.append(Work.WAITING_SUCCESSORS)
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            self.work_list.remove(Work.WAITING_SUCCESSORS)

            # signal the metadata store node that the action nodes have finish using the resource for the current run
            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS)
            
            # wait for acknowledgement from metadata store node that the run has been ended
            self.trap_interrupts()
            self.work_list.append(Work.WAITING_PREDECESSORS)
            while self.check_predecessors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            self.work_list.remove(Work.WAITING_PREDECESSORS)
            


class BaseActionNode(BaseNode):
    def __init__(self, name: str, predecessors: List[BaseNode], loggers: Union[Logger, List[Logger]] = None) -> None:
        super().__init__(name, predecessors, loggers=loggers)

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
            self.work_list.append(Work.WAITING_PREDECESSORS)
            while self.check_predecessors_signals() is False:
                time.sleep(0.2)
                self.trap_interrupts()
            self.work_list.remove(Work.WAITING_PREDECESSORS)
            
            self.trap_interrupts()
            self.work_list.append(Work.BEFORE_EXECUTION)
            self.before_execution()
            self.work_list.remove(Work.BEFORE_EXECUTION)

            ret = None
            try:
                self.trap_interrupts()
                self.work_list.append(Work.EXECUTION)
                ret = self.execute()
                self.work_list.remove(Work.EXECUTION)
                
                if ret:
                    self.trap_interrupts()
                    self.work_list.append(Work.ON_SUCCESS)
                    self.on_success()
                    self.work_list.remove(Work.ON_SUCCESS)

                else:
                    self.work_list.append(Work.ON_FAILURE)
                    self.trap_interrupts()
                    self.on_failure()
                    self.work_list.remove(Work.ON_FAILURE)

            except Exception as e:
                self.log(f"Error executing node '{self.name}': {traceback.format_exc()}")
                self.trap_interrupts()
                self.work_list.append(Work.ON_ERROR)
                self.on_error(e)
                self.work_list.remove(Work.ON_ERROR)

            finally:
                self.trap_interrupts()
                self.work_list.append(Work.AFTER_EXECUTION)
                self.after_execution()
                self.work_list.remove(Work.AFTER_EXECUTION)

            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS if ret else Result.FAILURE)

            # checking for successors signals before signalling predecessors will 
            # ensure all action nodes have finished using the resource for current run
            self.trap_interrupts()
            self.work_list.append(Work.WAITING_SUCCESSORS)
            while self.check_successors_signals() is False:
                time.sleep(0.2)
                self.trap_interrupts()
            self.work_list.remove(Work.WAITING_SUCCESSORS)

            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS if ret else Result.FAILURE)
