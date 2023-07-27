from enum import Enum

class Status(Enum):
    SUCCESS = 0,
    FAILURE = 1,
    RUNNING = 2,
    WAITING = 3,
    SKIPPED = 4,
    ERROR = 5,
    EXITED = 6

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