from __future__ import annotations

from threading import Thread, Lock
from queue import Queue, Empty
from typing import List, Any, Dict, Optional, Tuple, Callable, Set, Union
from functools import reduce
import time
import networkx as nx
from logging import Logger
import uuid
from uuid import UUID
from datetime import datetime

from pydantic import BaseModel

from .constants import Status, ASTOperation


class SignalAST:
    '''
    This Class Represents the boolean expression of signals
    required by a node class to trigger
    '''
    def __init__(self, operation:ASTOperation, parameters:List[Union[BaseNode, SignalAST]]):
        self.operation = operation
        self.parameters = parameters
    
    def evaluate(self, node:BaseNode) -> bool:
        '''
        Evaluate the AST based on the existance of signal and success (if it does exist) for the given node's received signals
        '''
        evaluated_params = list()
        for param in self.parameters:
            if isinstance(param, SignalAST):
                evaluated_params.append(param.evaluate(node))
            else:
                value = (param.name in node.received_signals) and (node.received_signals[param.name].status == Status.SUCCESS)
                evaluated_params.append(value)

        if self.operation == ASTOperation.NOT:
            assert len(evaluated_params) == 1
            return not evaluated_params[0]
        elif self.operation == ASTOperation.AND:
            return all(evaluated_params)
        elif self.operation == ASTOperation.OR:
            return any(evaluated_params)
        elif self.operation == ASTOperation.XOR:
            return reduce(lambda x, y: x^x, evaluated_params)
        else:
            raise ValueError(f"Invalid Operation: {self.operation}")

    def nodes(self) -> Set[BaseNode]:
        '''
        Retset a set of nodes that are in this AST and subtrees
        '''
        return set(self._nodes())

    def _nodes(self):
        '''
        Recursive helper for `nodes()` to return all nodes included in the AST
        '''
        for param in self.parameters:
            if isinstance(param, SignalAST):
                for n in param._nodes():
                    yield n
            else:
                yield param

def Not(n:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.NOT,
        parameters = [n]
    )

def And(*args:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.AND,
        parameters = args
    )

def Or(*args:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.OR,
        parameters = args
    )

def XOr(*args:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.XOR,
        parameters = args
    )

class Message(BaseModel):
    # TODO allow dynamic key-values
    # https://docs.pydantic.dev/latest/usage/models/#dynamic-model-creation
    sender: str
    signal_type:str
    timestamp: datetime
    status: Optional[Status] = None

class BaseNode(Thread):
    def __init__(self, 
        name: str, 
        signal_type: str = "DEFAULT_SIGNAL",
        listen_to: Union[Union[BaseNode, SignalAST], List[Union[BaseNode, SignalAST]]] = list(),
        auto_trigger: bool = True,

    ) -> None:
        '''
        :param name: a name given to the node
        :param signal_type: ???
        :param listen_to: The list of nodes or boolean expression of nodes (SignalAST) that this node requires signals of to trigger. Items in the list will be boolean AND'd together
        :param auto_trigger: If False, then the node requires another object to trigger 
        '''

        super().__init__()
        self.name = name
        self.signal_type = signal_type # TODO what are the different signal types. Does a node need to track this?
        self.auto_trigger = auto_trigger
        
        # use self.status(). Property is Thread Safe 
        self._status_lock = Lock()
        self._status = Status.OFF
        self._status_updated = False
        
        self.triggered = False

        self.dependent_nodes = set()
        
        if not isinstance(listen_to, list):
            listen_to = [listen_to]
        self.signal_ast = And(*listen_to)
        for item in listen_to:
            if isinstance(item, SignalAST):
                self.dependent_nodes |= item.nodes()
            else:
                self.dependent_nodes |= {item}

        # Nodes to signal
        self.next_nodes = list()

        # set next_nodes for each dependent node with self
        for node in self.dependent_nodes:
            node.next_nodes.append(self)

        # Queue of incoming signals from the dependent_nodes
        self.incoming_signals = Queue()

        # Store for signals after processing them (and in the future after acknowledging them too maybe?)
        # Only keeps the most recent signal received
        self.received_signals:Dict[str, Message] = dict()

        
        self.wait_time = 3
        self.throttle = .100
        self.logger = None

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name}, status: {str(self.status)})'"
    
    def __and__(self, other) -> SignalAST:
        '''Overwrites the & bitwise operator'''
        return And(self, other)

    def __or__(self, other) -> SignalAST:
        '''Overwrites the | bitwise operator'''
        return Or(self, other)

    def __xor__(self, other) -> SignalAST:
        '''Overwrites the ^ bitwise operator'''
        return XOr(self, other)

    def __invert__(self) -> SignalAST:
        '''Overwrites the ~ bitwise operator'''
        return Not(self)

    def set_logger(self, logger: Logger) -> None:
        self.logger = logger
    
    def get_status(self) -> Status:
        return self.status
    
    def set_status(self, status: Status) -> None:
        self.status = status

    def log(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)
        else:
            print(message)

    def setup(self) -> None:
        # override to specify actions needed to create node.
        # such actions can include pulling and setting up docker containers, 
        # creating python virtual environments, creating database connections, etc.
        # note that the setup() method will be ran in a separate thread; 
        # this is the main difference between setting up the node using setup() and __init__()
        # therefore, it is best to put set up logic here that is not dependent on other nodes.
        pass

    @self.pausable
    def pre_check(self) -> bool:
        # should be used for continuously checking if the node is ready to start
        # i.e., checking if database connections, API connections, etc. are ready 
        return True

    @self.pausable
    def check_signals(self) -> bool:
        '''
        Verify all received signal statuses match the condition for this node to execute
        '''
        # Pull out the queued up incoming signals and register them
        while not self.incoming_signals.empty():
            sig = self.incoming_signals.get()
            self.received_signals[sig.sender] = sig
            # TODO For signaling over the network, this is where we'd send back an ACK

        # Check if the signals match the execute condition
        return self.signal_ast.evaluate(self)

    @self.pausable
    def pre_execution(self) -> None:
        # override to enable node to do something before execution; 
        # e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        pass

    @self.pausable
    def execute(self, *args, **kwargs) -> bool:
        # the logic for a particular stage in the MLOps pipeline
        pass

    @self.pausable
    def post_execution(self) -> None:
        pass

    @self.pausable
    def on_success(self) -> None:
        # override to enable node to do something after execution in event of success of action_function; 
        # e.g., send an email to the data science team to let everyone know the pipeline has finished training a new model
        pass

    @self.pausable
    def on_failure(self, e: Exception = None) -> None:
        # override to enable node to do something after execution in event of failure of action_function; 
        # e.g., send an email to the data science team to let everyone know the pipeline has failed to train a new model
        pass
    
    @self.pausable
    def send_signals(self, status:Status):
        msg = Message(
            sender = self.name,
            signal_type = self.signal_type,
            timestamp = datetime.now(),
            status = status
        )

        for n in self.next_nodes:
            n.incoming_signals.put(msg)

    def teardown(self) -> None:
        # override to specify actions to be executed upon removal of node from dag or on pipeline shutdown
        pass

    def reset_trigger(self):
        # TODO reset trigger dependent on the state of the system i.e. data store, feature store, model store
        self.triggered = False

    @property
    def status(self):
        self._status_lock.acquire()
        s = self._status
        self._status_lock.release()
        return s

    @status.setter
    def status(self, value: Status):
        self._status_lock.acquire()
        self._status = value
        self._status_lock.release()

    def pausable(self, func):
        '''
        A Decorator for allowing execution in the Status.RUNNING state to be paused mid execution
        '''
        def wrapper(*args, **kwargs):
            ret = func(*args, **kwargs)

            while self.status == Status.PAUSED:
                time.sleep(self.wait_time)

            return ret
        return wrapper

    def pause(self):
        self.status = Status.PAUSED

    def resume(self):
        if self.status == Status.PAUSED:
            self.status = Status.RUNNING

    def stop(self):
        self.status = Status.STOPPING

    def force_stop(self):
        # TODO
        pass


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
            if self.status == Status.RUNNING and not self.triggered:

                # TODO conditiona on auto-trigger

                # If pre-check fails, then just wait and try again
                if not self.pre_check():
                    self.status = Status.WAITING
                    continue

                # If not all signals received / boolean statement of signals is false
                # wait and try again
                if not self.check_signals():
                    self.status = Status.WAITING
                    continue
                
                # Precheck is good and the signals we want are good
                self.pre_execution()

                # Run the action function
                try:
                    self.triggered = True
                    ret = self.execute()
                    
                    if ret:
                        self.on_success()        
                        self.post_execution()
                        self.send_signals(Status.SUCCESS)
                    else:
                        self.on_failure()
                        self.post_execution()
                        self.send_signals(Status.FAILURE)
                except Exception as e:
                    self.on_failure(e)
                    self.post_execution()
                    self.send_signals(Status.FAILURE)
                    #todo go to errored? conditional on user set var

                # Commented out until other parts of the project are built out
                # self.reset_trigger()

                self.status == Status.COMPLETED

            elif self.status == Status.PAUSED or self.status == Status.COMPLETED:
                # Stay Indefinitely Paused until external action
                time.sleep(self.wait_time)

            elif self.status == Status.WAITING:
                # Sleep and then start running again
                time.sleep(self.wait_time)
                self.status = Status.RUNNING

            elif self.status == Status.STOPPING:
                # TODO lock status lock? disable state change here. force into closing
                # TODO release locks
                # TODO release resources
                # TODO maybe annouce to other nodes we have stopped?
                self.status = Status.EXITED

            if self.status == Status.EXITED:
                break

            time.sleep(self.throttle)

class TrueNode(BaseNode):
    '''A Node that does nothing and always returns a success'''
    def execute(self):
        return True

    def setup(self):
        time.sleep(2)

class FalseNode(BaseNode):
    '''A Node that does nothing and always returns a failure'''
    def execute(self):
        return False

    def setup(self):
        time.sleep(1)


class ActionNode(BaseNode):
    def __init__(self, name: str, signal_type: str, listen_to: List[BaseNode] = []) -> None:
        super().__init__(name, signal_type, listen_to, auto_trigger=True)


class ResourceNode(BaseNode):
    def __init__(self, name: str, signal_type: str) -> None:
        super().__init__(name, signal_type)
