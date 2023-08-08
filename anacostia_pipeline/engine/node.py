from multiprocessing import Queue, Lock, Value
from typing import List, Any, Dict, Optional
import time
import networkx as nx
from logging import Logger
import uuid
from uuid import UUID
from datetime import datetime

from pydantic import BaseModel

from .constants import Status
from .SignalAST import SignalAST, Not, And, Or, XOr

class Message(BaseModel):
    sender: str
    signal_type:str
    timestamp: datetime
    status: Optional[Status] = None

class BaseNode:

    def __init__(self, 
        name: str, 
        signal_type: str,
        listen_to: List['BaseNode'] = [],
        auto_trigger: bool = False,
    ) -> None:

        self.id = uuid.uuid4()
        self.name = name
        self.signal_type = signal_type # TODO what are the different signal types. Does a node need to track this?
        self.listeners = listen_to
        self.auto_trigger = auto_trigger # TODO what is this for exactly?
        self.status = STATUS.OFF
        self.triggered = False
        self.incoming_signals = Queue()
        self.signals:Dict[BaseNode, Message] = dict()
        self.wait_time = 3

        self.logger = None

    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        return f"'Node(name: {self.name}, uuid: {str(self.id)})'"
    
    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __xor__(self, other):
        return XOr(self, other)

    def __invert__(self):
        return Not(self)

    def set_logger(self, logger: Logger) -> None:
        self.logger = logger
    
    def log(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)
        else:
            print(message)

    def signal_message_template(self) -> Message:
        '''Returns a Template for a Signal Message'''
        return Message(
            sender = self.name,
            signal_type = self.signal_type,
            timestamp = datetime.now(),
            status = None
        )

    def setup(self) -> None:
        # override to specify actions needed to create node.
        # such actions can include pulling and setting up docker containers, 
        # creating python virtual environments, creating database connections, etc.
        # note that the setup() method will be ran in a separate thread; 
        # this is the main difference between setting up the node using setup() and __init__()
        # therefore, it is best to put set up logic here that is not dependent on other nodes.
        pass

    def pre_check(self) -> bool:
        # should be used for continuously checking if the node is ready to start
        # i.e., checking if database connections, API connections, etc. are ready 
        return True

    def check_signals(self) -> bool:
        # TODO pull in all signals from self.incoming_signals

        # TODO verify if all the signals we need are there

    def pre_execution(self) -> None:
        # override to enable node to do something before execution; 
        # e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        pass

    def execute(self) -> bool:
        # the logic for a particular stage in the MLOps pipeline
        pass

    def post_execution(self) -> None:
        pass

    def on_success(self) -> None:
        # override to enable node to do something after execution in event of success of action_function; 
        # e.g., send an email to the data science team to let everyone know the pipeline has finished training a new model
        pass

    def on_failure(self, e:Exception=None) -> None:
        # override to enable node to do something after execution in event of failure of action_function; 
        # e.g., send an email to the data science team to let everyone know the pipeline has failed to train a new model
        pass
    
    def send_signals(self, status:Status):
        pass

    def teardown(self) -> None:
        # override to specify actions to be executed upon removal of node from dag or on pipeline shutdown
        pass

    def reset_trigger(self):
        self.triggered = False

    def run(self, run_flag: Value) -> None:
        self.status = Status.RUNNING
        try:
            self.setup()
        except Exception as e:
            print(f"{str(self)} setup failed: {e}")
            self.status = Status.ERROR
            return

        while True:
            if self.status == Status.RUNNING:               
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
                    ret = self.execute()
                    if ret:
                        self.on_success()
                        self.post_execution()
                        self.send_signals(status.SUCCESS)
                    else:
                        self.on_failure()
                        self.post_execution()
                        self.send_signals(status.FAILURE)
                except Exception as e:
                    self.on_failure(e)
                    self.post_execution()
                    self.send_signals(status.FAILURE)

                self.reset_trigger()

            elif self.state == PAUSED:
                # Stay Indefinitely Paused until external action
                time.sleep(self.wait_time)

            elif self.state == WAITING:
                # Sleep and then start running again
                time.sleep(self.wait_time)
                self.state = RUNNING

            elif self.state == Status.STOPPING:
                # TODO release locks
                # TODO release resources
                # TODO maybe annouce to other nodes we have stopped?
                self.state = Status.EXITED

            if self.state == Status.EXITED:
                break

class TrueNode(BaseNode):
    '''A Node that does nothing and always returns a success'''
    def execute(self):
        return True

class FalseNode(BaseNode):
    '''A Node that does nothing and always returns a failure'''
    def execute(self):
        return False


class ActionNode(BaseNode):
    def __init__(self, name: str, signal_type: str, listen_to: List[BaseNode] = []) -> None:
        super().__init__(name, signal_type, listen_to, auto_trigger=True)


class ResourceNode(BaseNode):
    def __init__(self, name: str, signal_type: str) -> None:
        super().__init__(name, signal_type)
