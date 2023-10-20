from typing import List

if __name__ == "__main__":
    from base import BaseActionNode, BaseNode
else:
    from engine.base import BaseActionNode, BaseNode



class AndAndNode(BaseActionNode):
    """
    AndAndNode does the following: 
        1. waits for all predecessors before signalling all successors.
        2. waits for all successors before signalling all predecessors.
    It is useful for synchronizing ActionNodes to wait for all predecessors to finish before executing.
    It is also useful for synchronizing ActionNodes with ResourceNodes 
    to ensure all ActionNodes finish executing before ResourceNodes update their respective state.
    """
    def __init__(self, name: str, predecessors: List[BaseNode]) -> None:
        super().__init__(name, predecessors)


class AndOrNode(BaseActionNode):
    pass


class OrAndNode(BaseActionNode):
    pass


class OrOrNode(BaseActionNode):
    pass