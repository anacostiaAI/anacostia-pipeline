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
            Status.STOPPING: "STOPPING"
        }
        return status_words[self]
    
    def __int__(self) -> int:
        status_ints = {
            Status.SUCCESS: 0,
            Status.FAILURE: 1,
            Status.RUNNING: 2,
            Status.WAITING: 3,
            Status.SKIPPED: 4,
            Status.ERROR: 5,
            Status.EXITED: 6,
            Status.OFF: 7,
            Status.PAUSING: 8,
            Status.PAUSED: 9,
            Status.STOPPING: 10
        }
        return status_ints[self]

class ASTOperation(Enum):
    NOT = 0
    AND = 1
    OR = 2
    XOR = 3