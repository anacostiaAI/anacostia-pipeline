from threading import Lock
from datetime import datetime
from pydantic import BaseModel
from typing import List

from .constants import Result

class Signal(BaseModel):
    sender: str
    receiver: str
    timestamp: datetime
    result: Result = None

    def __repr__(self) -> str:
        return f"Signal (sender: {self.sender}, receiver: {self.receiver}, timestamp: {str(self.timestamp)}, result: {self.result})"
