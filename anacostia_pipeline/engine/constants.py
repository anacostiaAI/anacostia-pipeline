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
    INIT = 11

    def __repr__(self) -> str:
        status_words = {
            Status.SUCCESS: "SUCCESS",
            Status.FAILURE: "FAILURE",
            Status.RUNNING: "RUNNING",
            Status.WAITING: "WAITING",
            Status.SKIPPED: "SKIPPED",
            Status.ERROR: "ERROR",
            Status.EXITED: "EXITED"
        }
        return status_words[self]

class ASTOperation(Enum):
    NOT = 0
    AND = 1
    OR = 2
    XOR = 3
