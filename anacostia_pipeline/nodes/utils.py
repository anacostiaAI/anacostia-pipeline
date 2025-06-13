from typing import List
from pydantic import BaseModel, ConfigDict


class NodeModel(BaseModel):
    '''
    A Pydantic Model for validation and serialization of a BaseNode
    '''
    model_config = ConfigDict(from_attributes=True)

    name: str
    node_type: str
    base_type: str
    predecessors: List[str]
    successors: List[str]


class NodeConnectionModel(NodeModel):
    """
    A Pydantic Model for validation and serialization of a BaseNode that is used to connect to a remote service.
    """
    node_url: str