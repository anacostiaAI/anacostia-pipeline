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
import asyncio
import httpx
import time

from anacostia_pipeline.utils.constants import Status, Result
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.connector import Connector
from anacostia_pipeline.nodes.rpc import BaseRPCCallee



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
        self, 
        name: str, 
        predecessors: List[BaseNode] = None, 
        remote_predecessors: List[str] = None,      # should be a list of urls or BaseRPCCaller
        remote_successors: List[str] = None, 
        caller_url: str = None,
        wait_for_connection: bool = False,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:

        self._status_lock = Lock()
        self.caller_url = caller_url
        self.wait_for_connection = wait_for_connection
        
        if loggers is None:
            self.loggers: List[Logger] = list()
        else:
            if isinstance(loggers, Logger):
                self.loggers: List[Logger] = [loggers]
            else:
                self.loggers: List[Logger] = loggers
        
        self.predecessors = list() if predecessors is None else predecessors
        self.remote_predecessors = list() if remote_predecessors is None else remote_predecessors
        self.predecessors_events: Dict[str, Event] = {predecessor.name: Event() for predecessor in self.predecessors}

        self.successors: List[BaseNode] = list()
        self.remote_successors = list() if remote_successors is None else remote_successors
        self.successor_events: Dict[str, Event] = {url: Event() for url in self.remote_successors}

        # add node to each predecessor's successors list and create an event for each predecessor's successor_events
        for predecessor in self.predecessors:
            predecessor.successors.append(self)
            predecessor.successor_events[name] = Event()

        self.exit_event = Event()
        self.pause_event = Event()
        self.connection_event = Event()
        self.pause_event.set()
        self.queue: Queue | None = None
        self.gui: BaseGUI | None = None
        self.connector: Connector | None = None

        super().__init__(name=name)
    
    def add_remote_predecessor(self, url: str):
        if url not in self.remote_predecessors:
            self.remote_predecessors.append(url)
            self.predecessors_events[url] = Event()
    
    def setup_connector(self, host: str, port: int):
        self.connector = Connector(self, host=host, port=port)
        return self.connector

    def setup_node_GUI(self, host: str, port: int):
        self.gui = BaseGUI(node=self, host=host, port=port)
        return self.gui
    
    def get_node_gui(self):
        if self.gui is None:
            raise ValueError("Node GUI not set up")
        return self.gui
    
    def setup_rpc_callee(self, host: str, port: int):
        self.rpc_callee = BaseRPCCallee(self, caller_url=self.caller_url, host=host, port=port, loggers=self.loggers)
        return self.rpc_callee

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

                    self.queue.put_nowait(
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
    
    def __signal_local_predecessors(self):
        if len(self.predecessors) > 0:
            for predecessor in self.predecessors:
                predecessor.successor_events[self.name].set()
            # self.log(f"'{self.name}' finished signalling local predecessors", level="INFO")
    
    def __signal_local_successors(self):
        if len(self.successors) > 0:
            for successor in self.successors:
                successor.predecessors_events[self.name].set()
            # self.log(f"'{self.name}' finished signalling local successors", level="INFO")
    
    async def __signal_remote_predecessors(self):
        if len(self.remote_predecessors) > 0:
            try:
                async with httpx.AsyncClient() as client:
                    tasks = []
                    for predecessor_url in self.remote_predecessors:
                        json = {
                            "node_url": f"http://{self.connector.host}:{self.connector.port}/{self.name}",
                            "node_name": self.name,
                            "node_type": type(self).__name__
                        }
                        tasks.append(client.post(f"{predecessor_url}/connector/backward_signal", json=json))

                    await asyncio.gather(*tasks)
                    self.log(f"'{self.name}' finished signalling remote predecessors", level="INFO")
            except httpx.ConnectError:
                self.log(f"'{self.name}' failed to signal predecessors", level="ERROR")
                self.exit()
    
    async def __signal_remote_successors(self):
        if len(self.remote_successors) > 0:
            try:
                async with httpx.AsyncClient() as client:
                    tasks = []
                    for successor_url in self.remote_successors:
                        json = {
                            "node_url": f"http://{self.connector.host}:{self.connector.port}/{self.name}",
                            "node_name": self.name,
                            "node_type": type(self).__name__
                        }
                        tasks.append(client.post(f"{successor_url}/connector/forward_signal", json=json))
                    
                    await asyncio.gather(*tasks)
                    self.log(f"'{self.name}' finished signalling remote successors", level="INFO")
            except httpx.ConnectError:
                self.log(f"'{self.name}' failed to signal successors from {self.name}", level="ERROR")
                self.exit()
    
    async def signal_successors(self, result: Result):
        # self.log(f"'{self.name}' signaling local successors", level="INFO")
        self.__signal_local_successors()

        # self.log(f"'{self.name}' signaling remote successors", level="INFO")
        await self.__signal_remote_successors()

    def wait_for_successors(self):
        # self.log(f"'{self.name}' waiting for successors", level="INFO")
        for event in self.successor_events.values():
            event.wait()
        
        # self.log(f"'{self.name}' finished waiting for successors", level="INFO")
        for event in self.successor_events.values():
            event.clear()
    
    async def signal_predecessors(self, result: Result):
        # self.log(f"'{self.name}' signaling local predecessors", level="INFO")
        self.__signal_local_predecessors()
        
        # self.log(f"'{self.name}' signaling remote predecessors", level="INFO")
        await self.__signal_remote_predecessors()

    def wait_for_predecessors(self):
        # self.log(f"'{self.name}' waiting for predecessors", level="INFO")
        for event in self.predecessors_events.values():
            event.wait()
        
        #self.log(f"'{self.name}' finished waiting for predecessors", level="INFO")
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
        self.connection_event.set()
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
        pass
    
    def root_setup(self):
        self.status = Status.INITIALIZING
        self.setup()
    
    def leaf_setup(self):
        self.status = Status.INITIALIZING
        self.setup()

        if self.wait_for_connection:
            self.log(f"'{self.name}' waiting for root predecessors to connect", level='INFO')
            
            # this event is set by the LeafPipeline when all root predecessors are connected and after it adds to predecessors_events
            self.connection_event.wait()
            if self.exit_event.is_set(): return

            self.log(f"'{self.name}' connected to root predecessors {list(self.predecessors_events.keys())}", level='INFO')

    async def run_async(self) -> None:
        """
        override to specify the logic of the node.
        """
        raise NotImplementedError
    
    def run(self) -> None:
        asyncio.run(self.run_async())