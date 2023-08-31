from __future__ import annotations

from threading import Thread, Lock, Event, RLock
from typing import List, Dict, Optional, Union
from functools import wraps
import time
from logging import Logger
import networkx as nx

if __name__ == "__main__":
    from constants import Status
else:
    from engine.constants import Status


G = nx.DiGraph()


class BaseNode(Thread):
    def __init__(self, 
        name: str, 
        listen_to: Union[BaseNode, List[BaseNode]] = list(),
        auto_trigger: bool = True,
    ) -> None:
        '''
        :param name: a name given to the node
        :param listen_to: The list of nodes or boolean expression of nodes (SignalAST) that this node requires signals of to trigger. Items in the list will be boolean AND'd together
        :param auto_trigger: If False, then the node requires another object to trigger 
        '''

        super().__init__()
        self.name = name

        # auto_trigger is used to determine if the node should be triggered automatically or not
        # if auto_trigger is True, then triggered will always be set to True
        # note that we don't need a lock for auto_trigger and triggered because it will only be accessed in the same thread
        self.auto_trigger = auto_trigger
        self.triggered = auto_trigger
        
        # use self.status(). Property is Thread Safe 
        self._status_lock = Lock()
        self._status = Status.OFF

        # in the future, rename resource_lock to state_lock
        self.state_lock = RLock()
        self.successor_event = Event()
        self.predecessor_event = Event()

        if not isinstance(listen_to, list):
            self.predecessors = [listen_to]
        else:
            self.predecessors = listen_to
        self.num_predecessors = len(self.predecessors)

        # self.successors and self.num_successors are set by the pipeline after the DAG is constructed
        # self.successors: List[BaseNode] = None
        self.successors = None
        self.num_successors = None
        self.reference_count = 0
    
        self.wait_time = 0.1 
        self.logger = None

        G.add_node(self)
        for predecessor in listen_to:
            G.add_edge(predecessor, self)

    @property
    def status(self) -> Status:
        while True:
            with self._status_lock:
                return self._status

    @status.setter
    def status(self, value: Status) -> None:
        while True:
            with self._status_lock:
                self._status = value
                return

    @staticmethod
    def pausable(func):
        '''
        A Decorator for allowing execution in the Status.RUNNING state to be paused mid execution
        '''
        def wrapper(self, *args, **kwargs):
            ret = func(self, *args, **kwargs)

            while self.status == Status.PAUSED:
                time.sleep(self.wait_time)

            return ret
        return wrapper

    def poll_predecessors(self):
        if self.num_predecessors > 0:
            for _ in range(self.num_predecessors):
                self.predecessor_event.wait()

    def signal_predecessors(self):
        for predecessor in self.predecessors:
            predecessor.successor_event.set()

    def clear_predecessor_event(self):
        if self.predecessor_event.is_set():
            self.predecessor_event.clear()

    def poll_successors(self):
        if self.num_successors > 0:
            for _ in range(self.num_successors):
                self.successor_event.wait()

    def signal_successors(self):
        for successor in self.successors:
            successor.predecessor_event.set()
    
    def clear_successor_event(self):
        if self.successor_event.is_set():
            self.successor_event.clear()

    def await_funcs(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            
            self.reference_count += 1

            result = func(self, *args, **kwargs)
            
            self.reference_count -= 1
            if self.reference_count == 0:
                self.successor_event.set()

            return result
        return wrapper
    
    def lock_decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # make sure setup is finished before allowing other nodes to access the resource
            if func.__name__ == "setup":
                # keep trying to acquire lock until function is finished
                # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
                while True:
                    with self.state_lock:
                        return func(self, *args, **kwargs)
            else:
                while self.status == Status.INIT:
                    time.sleep(self.wait_time)

                while True:
                    with self.state_lock:
                        return func(self, *args, **kwargs)
        return wrapper

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

    def setup(self) -> None:
        pass

    @pausable
    def pre_check(self) -> bool:
        return True

    @pausable
    def pre_execution(self) -> None:
        pass

    @pausable
    def execute(self, *args, **kwargs) -> bool:
        return True

    @pausable
    def post_execution(self) -> None:
        pass
    
    @pausable
    def on_success(self) -> None:
        pass

    @pausable
    def on_failure(self, e: Exception = None) -> None:
        pass

    def on_exit(self):
        pass

    def teardown(self) -> None:
        pass

    def trigger(self) -> None:
        self.triggered = True

    def reset_trigger(self):
        # only reset trigger if auto_trigger is False
        if self.auto_trigger == False:
            self.triggered = False

    def run(self) -> None:
        self.status = Status.INIT
        try:
            self.setup()
        except Exception as e:
            print(f"{str(self)} setup failed: {e}")
            self.status = Status.ERROR
            return

        self.status = Status.RUNNING

        while True:
            if self.status == Status.RUNNING:               
                if self.triggered:

                    # If pre-check fails, then just wait and try again
                    if not self.pre_check():
                        self.status = Status.WAITING
                        continue
                    
                    # wait for predecessor to be done executing 
                    self.status = Status.WAITING
                    self.poll_predecessors()
                    self.status = Status.RUNNING

                    try:
                        # Precheck is good and the signals we want are good
                        self.pre_execution()

                        ret = self.execute()
                        if ret:
                            self.on_success()
                            self.post_execution()
                        else:
                            self.on_failure()
                            self.post_execution()

                    except Exception as e:
                        self.on_failure(e)
                        self.post_execution()

                    self.signal_successors()
                    self.clear_predecessor_event()
                    self.reset_trigger()    

            elif self.status == Status.PAUSED:
                # Stay Indefinitely Paused until external action
                time.sleep(self.wait_time)

            elif self.status == Status.WAITING:
                # Sleep and then start running again
                time.sleep(self.wait_time)
                self.status = Status.RUNNING

            elif self.status == Status.STOPPING:
                self.on_exit()
                self.status = Status.EXITED

            if self.status == Status.EXITED:
                break

            time.sleep(self.wait_time)