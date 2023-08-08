from typing import List
from functools import reduce

from pydantic import BaseModel

from .constants import Status, ASTOperation
from .node import BaseNode

class SignalAST(BaseModel):
    '''
    This Class Represents the boolean expression of signals
    required by a node class to trigger
    '''
    operation:ASTOperation
    parameters:List[BaseNode]
    
    def evaluate(self. node:BaseNode) -> bool:
        '''
        Evaluate the AST based on the existance of signal and success (if it does exist) for the given node
        '''
        evaluated_params = list()
        for param in self.parameters:
            if isinstance(param, SignalAST):
                evaluated_params.append(param.evaluate(node))
            else:
                value = (param in node.signals) and (node.signals[param].status == Status.SUCCESS)
                evaluated_params.append(value)

        if operation == NOT:
            assert len(evaluated_params) == 1
            return not evaluated_params[0]
        elif operation == AND:
            return all(evaluated_params)
        elif operation == OR:
            return any(evaluated_params)
        elif operation == XOR:
            return reduce(lambda x, y: x^x, evaluated_params)
        else:
            raise ValueError(f"Invalid Operation: {operation}")

def Not(n:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.NOT,
        parameters = [n]
    )

def And(*args:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.AND,
        parameters = args
    )

def Or(*args:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.OR,
        parameters = args
    )

def XOr(*args:Union[SignalAST, BaseNode]):
    return SignalAST(
        operation = ASTOperation.XOR,
        parameters = args
    )