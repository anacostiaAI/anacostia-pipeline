from threading import Lock
from datetime import datetime
from pydantic import BaseModel
from typing import List

if __name__ == "__main__":
    from constants import Status, Result, Work
else:
    from engine.constants import Status, Result, Work



class Signal(BaseModel):
    sender: str
    receiver: str
    timestamp: datetime
    result: Result = None

    def __repr__(self) -> str:
        return f"Signal (sender: {self.sender}, receiver: {self.receiver}, timestamp: {str(self.timestamp)}, result: {self.result})"



class SignalTable:
    def __init__(self) -> None:
        self.table = dict()
        self.table_lock = Lock()

    def __getitem__(self, key: str) -> Signal:
        while True:
            with self.table_lock:
                return self.table[key]

    def __setitem__(self, key: str, value: Signal) -> None:
        while True:
            with self.table_lock:
                self.table[key] = value
                return

    def __delitem__(self, key) -> None:
        while True:
            with self.table_lock:
                del self.table[key]
                return

    def keys(self) -> List[str]:
        while True:
            with self.table_lock:
                return list(self.table.keys())

    def values(self) -> List[Signal]:
        while True:
            with self.table_lock:
                return list(self.table.values())

    def items(self) -> List[tuple]:
        while True:
            with self.table_lock:
                return list(self.table.items())
    
    def __len__(self) -> int:
        while True:
            with self.table_lock:
                return len(self.table)

    def __repr__(self) -> str:
        while True:
            with self.table_lock:
                return "TODO: implement this"
