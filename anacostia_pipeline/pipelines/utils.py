from pydantic import BaseModel


class EventModel(BaseModel):
    event: str
    data: str