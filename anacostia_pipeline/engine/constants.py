from enum import Enum

class Status(Enum):
    SUCCESS = 0,
    FAILURE = 1,
    RUNNING = 2,
    WAITING = 3,
    SKIPPED = 4,
    ERROR = 5,
    EXITED = 6,
    # Node.run() has not been executed yet
    OFF = 7,
    PAUSING = 8,
    PAUSED = 9
    STOPPING = 10
    INIT = 11,
    COMPLETED = 12

    def __repr__(self) -> str:
        status_words = {
            Status.SUCCESS: "SUCCESS",
            Status.FAILURE: "FAILURE",
            Status.RUNNING: "RUNNING",
            Status.WAITING: "WAITING",
            Status.SKIPPED: "SKIPPED",
            Status.ERROR: "ERROR",
            Status.EXITED: "EXITED",
            Status.OFF: "OFF",
            Status.PAUSING: "PAUSING",
            Status.PAUSED: "PAUSED",
            Status.STOPPING: "STOPPING",
            Status.INIT: "INITIALIZING",
            Status.COMPLETED: "COMPLETED"
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