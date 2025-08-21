from __future__ import annotations
from threading import Thread, Lock, Event
from queue import Queue
from typing import List, Union, Dict
from logging import Logger
from datetime import datetime
from functools import wraps
import traceback
import json
import httpx

from anacostia_pipeline.utils.constants import Status, Result
from anacostia_pipeline.nodes.utils import NodeModel
from anacostia_pipeline.nodes.gui import BaseGUI
from anacostia_pipeline.nodes.connector import Connector
from anacostia_pipeline.nodes.api import BaseServer



class BaseNode(Thread):
    def __init__(
        self, 
        name: str, 
        predecessors: List[BaseNode] = None, 
        remote_predecessors: List[Dict[str, str]] = None, 
        remote_successors: List[Dict[str, str]] = None, 
        client_url: str = None,
        wait_for_connection: bool = False,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:

        """
        Base class for all nodes in the pipeline.
        Args:
            name (str): Name of the node.
            predecessors (List[BaseNode], optional): List of predecessor nodes. Defaults to None.
            remote_predecessors (List[Dict[str, str]], optional): List of dictionaries containing information on certs.
                ```
                [
                    {
                        "node_url": "https://localhost:8000/node1", 
                        "ssl_keyfile": "path/to/keyfile.pem", 
                        "ssl_certfile": "path/to/certfile.pem", 
                        "ssl_ca_certs": "path/to/ca_certs.pem"
                    },
                    {
                        "node_url": "https://localhost:8001/node2", 
                        "ssl_keyfile": "path/to/keyfile.pem", 
                        "ssl_certfile": "path/to/certfile.pem", 
                        "ssl_ca_certs": "path/to/ca_certs.pem"
                    }
                ]
                ```
            remote_successors (List[Dict[str, str]], optional): Same as in remote_predecessors, but for remote successors.
            client_url (str, optional): URL of the BaseClient for the BaseServer of this node to connect to. Defaults to None.
            wait_for_connection (bool, optional): Whether to wait for connection. Defaults to False.
            loggers (Union[Logger, List[Logger]], optional): Logger or list of loggers for logging. Defaults to None.
        """

        self._status_lock = Lock()
        self.client_url = client_url
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
        self.node_server: BaseServer | None = None
        self.connector: Connector | None = None

        super().__init__(name=name)
    
    def add_remote_predecessor(self, url: str):
        if url not in self.remote_predecessors:
            self.remote_predecessors.append(url)
            self.predecessors_events[url] = Event()

    def setup_connector(
        self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None
    ) -> Connector:
        self.connector = Connector(
            node=self, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs, loggers=self.loggers
        )
        return self.connector

    def setup_node_GUI(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.gui = BaseGUI(node=self, host=host, port=port, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs)
        return self.gui
    
    def get_node_gui(self):
        if self.gui is None:
            raise ValueError("Node GUI not set up")
        return self.gui
    
    def setup_node_server(self, host: str, port: int, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_ca_certs: str = None):
        self.node_server = BaseServer(
            self, client_url=self.client_url, host=host, port=port, loggers=self.loggers, 
            ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_ca_certs=ssl_ca_certs
        )
        return self.node_server

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name})'"
    
    def model(self) -> NodeModel:
        pass

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
        while not self.exit_event.is_set():
            with self._status_lock:
                return self._status

    @status.setter
    def status(self, value: Status):
        while not self.exit_event.is_set():
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
    
    def signal_successors(self, result: Result):
        # self.log(f"'{self.name}' signaling local successors", level="INFO")
        if len(self.successors) > 0:
            for successor in self.successors:
                successor.predecessors_events[self.name].set()

        # self.log(f"'{self.name}' signaling remote successors", level="INFO")
        try:
            self.connector.signal_remote_successors()
            self.log(f"'{self.name}' finished signalling remote successors", level="INFO")
        except httpx.ConnectError:
            self.log(f"'{self.name}' failed to signal successors from {self.name}", level="ERROR")
            self.exit()

    def wait_for_successors(self):
        # self.log(f"'{self.name}' waiting for successors", level="INFO")
        for event in self.successor_events.values():
            event.wait()
        
        # self.log(f"'{self.name}' finished waiting for successors", level="INFO")
        for event in self.successor_events.values():
            event.clear()
    
    def signal_predecessors(self, result: Result):
        # self.log(f"'{self.name}' signaling local predecessors", level="INFO")
        if len(self.predecessors) > 0:
            for predecessor in self.predecessors:
                predecessor.successor_events[self.name].set()
        
        # self.log(f"'{self.name}' signaling remote predecessors", level="INFO")
        try:
            self.connector.signal_remote_predecessors()
            self.log(f"'{self.name}' finished signalling remote predecessors", level="INFO")
        except httpx.ConnectError:
            self.log(f"'{self.name}' failed to signal remote predecessors", level="ERROR")
            self.exit()

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
        self.log(f"Node '{self.name}' exiting at {datetime.now()}", level='INFO')
        
        # set all events so loop can continue to next checkpoint and break out of loop
        self.connection_event.set()
        self.pause_event.set()
        self.exit_event.set()

        for event in self.successor_events.values():
            event.set()

        for event in self.predecessors_events.values():
            event.set()

        self.log(f"Node '{self.name}' exited at {datetime.now()}", level='INFO')

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
    
    def run(self) -> None:
        raise NotImplementedError("run() method is to be implemented in BaseMetadataStoreNode, BaseResourceNode, and BaseActionNode.")