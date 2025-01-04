from __future__ import annotations
from threading import Thread, Lock, Event
from queue import Queue
from typing import List, Union, Optional, Dict
from logging import Logger
from datetime import datetime
from functools import wraps
import traceback
from pydantic import BaseModel, ConfigDict
import json

from anacostia_pipeline.utils.constants import Status, Result, Status
from anacostia_pipeline.nodes.app import BaseApp



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
    def __init__(self, name: str, predecessors: List[BaseNode] = None, loggers: Union[Logger, List[Logger]] = None) -> None:
        self._status_lock = Lock()
        self.work_set = set()
        
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
        self.predecessors_events: Dict[str, Event] = {predecessor.name: Event() for predecessor in self.predecessors}

        self.successors: List[BaseNode] = list()
        self.successor_events: Dict[str, Event] = {}

        # add node to each predecessor's successors list and create an event for each predecessor's successor_events
        for predecessor in self.predecessors:
            predecessor.successors.append(self)
            predecessor.successor_events[name] = Event()

        self.exit_event = Event()
        self.pause_event = Event()
        self.pause_event.set()
        self.queue: Queue | None = None
        self.app: BaseApp | None = None

        super().__init__(name=name)
    
    def get_app(self):
        self.app = BaseApp(self)
        return self.app

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

                # if pipeline app has set the queue, send a message to the queue when the status changes
                if self.queue is not None:
                    data = {
                        "id": self.name,
                        "status": repr(value)
                    }

                    self.queue.put(
                        {
                            "event": "WorkUpdate",
                            "data": json.dumps(data)
                        }
                    )

                break

    def set_queue(self, queue: Queue):
        self.queue = queue

    def log_exception(func):
        @wraps(func)
        def log_exception_wrapper(self: BaseNode, *args, **kwargs):
            try: 
                ret = func(self, *args, **kwargs)
                return ret
            except Exception as e:
                self.log(f"Error in user-defined method '{func.__name__}' of node '{self.name}': {traceback.format_exc()}", level="ERROR")
                return
        return log_exception_wrapper
    
    def signal_successors(self, result: Result):
        for successor in self.successors:
            successor.predecessors_events[self.name].set()

    def wait_for_successors(self):
        for event in self.successor_events.values():
            event.wait()
        
        for event in self.successor_events.values():
            event.clear()
    
    def signal_predecessors(self, result: Result):
        for predecessor in self.predecessors:
            predecessor.successor_events[self.name].set()

    def wait_for_predecessors(self):
        for event in self.predecessors_events.values():
            event.wait()
        
        for event in self.predecessors_events.values():
            event.clear()

    def pause(self):
        self.status = Status.PAUSED
        self.pause_event.clear()

    def resume(self):
        self.pause_event.set()

    def exit(self):
        # setting all events forces the loop to continue to the next checkpoint which will break out of the loop
        self.log(f"Node '{self.name}' exiting at {datetime.now()}")
        
        # set all events so loop can continue to next checkpoint and break out of loop
        self.pause_event.set()
        self.exit_event.set()

        for event in self.successor_events.values():
            event.set()
        
        for event in self.predecessors_events.values():
            event.set()

        self.log(f"Node '{self.name}' exited at {datetime.now()}")

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

    def run(self) -> None:
        """
        override to specify the logic of the node.
        """
        raise NotImplementedError
