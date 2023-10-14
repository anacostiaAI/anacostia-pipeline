from enum import Enum

class Status(Enum):
    OFF = 14
    INIT = 12,
    RUNNING = 6,
    SKIPPED = 7,
    PAUSING = 8,
    PAUSED = 9,
    EXITING = 10,
    EXITED = 11,
    DISCONNECTED = 15,
    ERROR = 2,

    def __repr__(self) -> str:
        status_words = {
            Status.RUNNING: "RUNNING",
            Status.SKIPPED: "SKIPPED",
            Status.PAUSING: "PAUSING",
            Status.PAUSED: "PAUSED",
            Status.EXITING: "EXITING",
            Status.EXITED: "EXITED",
            Status.INIT: "INITIALIZING",
            Status.OFF: "OFF",
            Status.DISCONNECTED: "DISCONNECTED",
            Status.ERROR: "ERROR",
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


class Work(Enum):
    WAITING_RESOURCE = 0,
    WAITING_SUCCESSORS = 1,
    WAITING_PREDECESSORS = 2,
    UPDATING_STATE = 3,
    BEFORE_EXECUTION = 4,
    EXECUTION = 5,
    AFTER_EXECUTION = 6
    ON_SUCCESS = 7,
    ON_FAILURE = 8,
    ON_ERROR = 9,
    WAITING_EXTERNAL_EVENT = 10,

    def __repr__(self) -> str:
        status_words = {
            Work.WAITING_RESOURCE: "WAITING_RESOURCE",
            Work.WAITING_SUCCESSORS: "WAITING_SUCCESSORS",
            Work.WAITING_PREDECESSORS: "WAITING_PREDECESSORS",
            Work.WAITING_EXTERNAL_EVENT: "WAITING_EXTERNAL_EVENT",
            Work.UPDATING_STATE: "UPDATE_STATE",
            Work.BEFORE_EXECUTION: "BEFORE_EXECUTION",
            Work.EXECUTION: "EXECUTION",
            Work.AFTER_EXECUTION: "AFTER_EXECUTION",
            Work.ON_SUCCESS: "ON_SUCCESS",
            Work.ON_FAILURE: "ON_FAILURE",
            Work.ON_ERROR: "ON_ERROR",
        }
        return status_words[self]
    
    def __int__(self) -> int:
        return self.value
    
    def __eq__(self, other: 'Work') -> bool:
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