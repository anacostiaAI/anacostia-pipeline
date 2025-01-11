from enum import Enum

class Status(Enum):
    WAITING_RESOURCE = 0,
    WAITING_METRICS = 1,
    QUEUED = 2,
    PREPARATION = 3,
    EXECUTING = 4,
    CLEANUP = 5
    COMPLETE = 6,
    FAILURE = 7,
    ERROR = 8,
    TRIGGERED = 9,
    INITIALIZING = 10,
    SKIPPED = 11,
    PAUSED = 12,

    def __repr__(self) -> str:
        status_words = {
            Status.WAITING_RESOURCE: "WAITING_RESOURCE",
            Status.WAITING_METRICS: "WAITING_METRICS",
            Status.QUEUED: "QUEUED",
            Status.PREPARATION: "PREPARATION",
            Status.EXECUTING: "EXECUTING",
            Status.CLEANUP: "CLEANUP",
            Status.COMPLETE: "COMPLETE",
            Status.FAILURE: "FAILURE",
            Status.ERROR: "ERROR",
            Status.TRIGGERED: "TRIGGERED",
            Status.INITIALIZING: "INITIALIZING",
            Status.SKIPPED: "SKIPPED",
            Status.PAUSED: "PAUSED",
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


class Result(Enum):
    FAILURE = 0,
    SUCCESS = 1,
    ERROR = 2,
    
    def __repr__(self) -> str:
        status_words = {
            Result.FAILURE: "FAILURE",
            Result.SUCCESS: "SUCCESS",
            Result.ERROR: "ERROR",
        }
        return status_words[self]

    def __int__(self) -> int:
        return self.value
    
    def __eq__(self, other: 'Result') -> bool:
        if other.value == self.value:
            return True
        else:
            return False
    
    def __hash__(self) -> int:
        return super().__hash__()