from enum import Enum

class Status(Enum):
    FAILURE = 0,
    SUCCESS = 1,
    ERROR = 2,
    WAITING_RESOURCE = 3,
    WAITING_SUCCESSORS = 4,
    WAITING_PREDECESSORS = 5,
    RUNNING = 6,
    SKIPPED = 7,
    PAUSING = 8,
    PAUSED = 9,
    EXITING = 10,
    EXITED = 11,
    INIT = 12,
    UPDATE_STATE = 13
    OFF = 14

    def __repr__(self) -> str:
        status_words = {
            Status.FAILURE: "FAILURE",
            Status.SUCCESS: "SUCCESS",
            Status.ERROR: "ERROR",
            Status.WAITING_RESOURCE: "WAITING_RESOURCE",
            Status.WAITING_SUCCESSORS: "WAITING_SUCCESSORS",
            Status.WAITING_PREDECESSORS: "WAITING_PREDECESSORS",
            Status.RUNNING: "RUNNING",
            Status.SKIPPED: "SKIPPED",
            Status.PAUSING: "PAUSING",
            Status.PAUSED: "PAUSED",
            Status.EXITING: "EXITING",
            Status.EXITED: "EXITED",
            Status.INIT: "INITIALIZING",
            Status.UPDATE_STATE: "UPDATE_STATE",
            Status.OFF: "OFF",
        }
        return status_words[self]
    
    def __int__(self) -> int:
        return self.value
    
    def __eq__(self, other: 'Status') -> bool:
        if other.value == self.value:
            return True
        else:
            return False
    
    def __hash__(self) -> int:
        return super().__hash__()

class ASTOperation(Enum):
    NOT = 0
    AND = 1
    OR = 2
    XOR = 3

class ExternalSignal(Enum):
    PAUSE = 0,
    RUN = 1,
    EXIT = 3,

    def __repr__(self) -> str:
        signal_words = {
            ExternalSignal.PAUSE: "PAUSE",
            ExternalSignal.RUN: "RUN",
            ExternalSignal.EXIT: "EXIT",
        }
        return signal_words[self]
    
    def __int__(self) -> int:
        return self.value
    
    def __eq__(self, other: 'ExternalSignal') -> bool:
        if other.value == self.value:
            return True
        else:
            return False
    
    def __hash__(self) -> int:
        return super().__hash__()