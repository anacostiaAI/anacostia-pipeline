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
import json

from jinja2.filters import FILTERS
from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

from pydantic import BaseModel, ConfigDict
from bs4 import BeautifulSoup

from .constants import Status, Result, Work
from .utils import Signal, SignalTable



class NodeModel(BaseModel):
    '''
    A Pydantic Model for validation and serialization of a BaseNode
    '''
    model_config = ConfigDict(from_attributes=True)

    name: str
    type: Optional[str]
    status: Union[Status, str]
    work: List[Work]
    predecessors: List[str]
    successors: List[str]



class NodeUIModel(NodeModel):
    url: Optional[str] = None
    endpoint: Optional[str] = None
    status_endpoint: Optional[str] = None
    work_endpoint: Optional[str] = None
    header_template: Optional[str] = None



class BaseNodeRouter(APIRouter):
    def __init__(self, node: BaseNode, header_template: str = None, use_default_router=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node
        self.header_template = header_template

        PACKAGE_NAME = "anacostia_pipeline"
        PACKAGE_DIR = os.path.dirname(sys.modules[PACKAGE_NAME].__file__)
        self.templates_dir = os.path.join(PACKAGE_DIR, "templates")
        self.templates = Jinja2Templates(directory=self.templates_dir)

        @self.get("/status", response_class=HTMLResponse)
        async def status_endpoint(request: Request):
            return f"Node status: {repr(self.node.status)}"

        if use_default_router is True:
            @self.get("/home", response_class=HTMLResponse)
            async def endpoint(request: Request):
                response = self.templates.TemplateResponse(
                    "basenode.html", 
                    {"request": request, "node": self.node.model(), "status_endpoint": self.get_status_endpoint()}
                )
                return response

    def model(self):
        return NodeUIModel(
            url = self.get_url(),
            endpoint = self.get_endpoint(),
            status_endpoint = self.get_status_endpoint(),
            work_endpoint = self.get_work_endpoint(),
            predecessors = [n.name for n in self.node.predecessors],
            successors = [n.name for n in self.node.successors],
            header_template = self.header_template
        )
    
    def get_url(self):
        return "127.0.0.1:8000"
    
    def get_prefix(self):
        return f"/node/{self.node.name}"

    def get_endpoint(self):
        return f"{self.get_prefix()}/home"
    
    def get_status_endpoint(self):
        return f"{self.get_prefix()}/status"
    
    def get_work_endpoint(self):
        return f"{self.get_prefix()}/work"
    
    def get_header_template(self):
        return self.header_template



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
    
    def get_router(self):
        return BaseNodeRouter(self)

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name})'"
    
    def model(self):
        return NodeModel(
            name = self.name,
            type = type(self).__name__,
            status = self.status.name,
            work = self.work_list,
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
        self,
        name: str,
        uri: str,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
    
        super().__init__(name, predecessors=[], loggers=loggers)
        self.uri = uri
        self.run_id = 0
        self.resource_lock = RLock()
    
    # TODO 
    # TEMPORARY SHORTERM SOLUTION
    # Replace with proper model() implementation that returns a BaseModel
    def html(self, templates, request):       
        data = dict()
        data["request"] = request
        data.update(self.model().dict())
        with open(self.uri, "r") as json_file:
            data.update(json.load(json_file))

        metadata_html = templates.TemplateResponse("metadatastore.html", data)
        return metadata_html

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
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)

            # creating a new run
            self.trap_interrupts()
            self.start_run()
            self.add_run_id()

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
            self.add_end_time()
            self.end_run()

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

    # TODO 
    # TEMPORARY SHORTERM SOLUTION
    # Replace with proper model() implementation that returns a BaseModel
    def html(self, templates, request):       
        data = dict()
        data["request"] = request
        data.update(self.model().dict())
        artifact_path = self.metadata_store.resource_uri(self)
        
        with open(artifact_path, "r") as json_file:
            data.update(json.load(json_file))

        return templates.TemplateResponse("resourcenode.html", data)

    def resource_accessor(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                with self.resource_lock:
                    result = func(self, *args, **kwargs)
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

            # signalling to all successors that the resource is ready to be used
            self.trap_interrupts()
            self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the current state before updating the state of the resource
            # (note: change status to Work.WAITING_SUCCESSORS)
            self.trap_interrupts()
            while self.check_successors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)

            # signal the metadata store node that the resource has been used for the current run
            self.trap_interrupts()
            self.signal_predecessors(Result.SUCCESS)
            
            # wait for acknowledgement from metadata store node that the run has been ended
            self.trap_interrupts()
            while self.check_predecessors_signals() is False:
                self.trap_interrupts()
                time.sleep(0.2)
            


class BaseActionNode(BaseNode):
    def __init__(self, name: str, predecessors: List[BaseNode], loggers: Union[Logger, List[Logger]] = None) -> None:
        super().__init__(name, predecessors, loggers=loggers)

    # TODO 
    # TEMPORARY SHORTERM SOLUTION
    # Replace with proper model() implementation that returns a BaseModel
    def html(self, templates, request):       
        data = dict()
        data["request"] = request
        data.update(self.model().dict())

        # TODO dynamically include ActionNode's output
        # Note: extract the path of train_plot.html and val_plot.html from the plot store
        # Note: this is a base action node, move chart rendering code somewhere else
        train_plot = "./train_plot.html"
        if os.path.exists(train_plot):
            with open(train_plot, 'r') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            chart = soup.find('div')
            data['train_chart'] = chart

        validation_plot = "./val_plot.html"
        if os.path.exists(validation_plot):
            with open(validation_plot, 'r') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            chart = soup.find('div')
            data['validation_chart'] = chart
        
        return templates.TemplateResponse("actionnode.html", data)


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
