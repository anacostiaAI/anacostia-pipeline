from pydantic import BaseModel



class ConnectionModel(BaseModel):
    root_name: str
    leaf_host: str
    leaf_port: int
    root_host: str
    root_port: int
    sender_name: str
    receiver_name: str


class EventModel(BaseModel):
    event: str
    data: str