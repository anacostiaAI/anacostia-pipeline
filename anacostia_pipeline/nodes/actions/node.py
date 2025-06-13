from typing import List, Union
from logging import Logger
import traceback

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.utils.constants import Result, Status
from anacostia_pipeline.nodes.utils import NodeModel



class BaseActionNode(BaseNode):
    def __init__(
        self, 
        name: str, 
        predecessors: List[BaseNode], 
        remote_predecessors: List[str] = None, 
        remote_successors: List[str] = None,
        client_url: str = None,
        wait_for_connection: bool = False,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
        super().__init__(
            name, predecessors, remote_predecessors=remote_predecessors, 
            remote_successors=remote_successors, client_url=client_url, wait_for_connection=wait_for_connection, loggers=loggers
        )

    def model(self) -> NodeModel:
        return NodeModel(
            name = self.name,
            node_type = type(self).__name__,
            base_type = "BaseActionNode",
            predecessors = [n.name for n in self.predecessors],
            successors = [n.name for n in self.successors]
        )

    @BaseNode.log_exception
    def before_execution(self) -> None:
        """
        override to enable node to do something before execution; 
        e.g., send an email to the data science team to let everyone know the pipeline is about to train a new model
        """
        pass

    @BaseNode.log_exception
    def after_execution(self) -> None:
        """
        override to enable node to do something after executing the action function regardless of the outcome of the action function; 
        e.g., send an email to the data science team to let everyone know the pipeline is done training a new model
        """
        pass

    @BaseNode.log_exception
    async def execute(self, *args, **kwargs) -> bool:
        """
        the logic for a particular stage in your MLOps pipeline
        """
        raise NotImplementedError

    @BaseNode.log_exception
    def on_failure(self) -> None:
        """
        override to enable node to do something after execution in event of failure of action_function; 
        e.g., send email to the data science team to let everyone know the pipeline has failed to train a new model
        """
        pass
    
    @BaseNode.log_exception
    def on_error(self, e: Exception) -> None:
        """
        override to enable node to do something after execution in event of error of action_function; 
        e.g., send email to the data science team to let everyone know the pipeline has failed to train a new model due to an error
        """
        pass
    
    @BaseNode.log_exception
    def on_success(self) -> None:
        """
        override to enable node to do something after execution in event of success of action_function; 
        e.g., send an email to the data science team to let everyone know the pipeline has finished training a new model
        """
        pass

    async def run_async(self) -> None:
        if self.wait_for_connection:
            self.log(f"'{self.name}' waiting for root predecessors to connect", level='INFO')
            
            # this event is set by the LeafPipeline when all root predecessors are connected and after it adds to predecessors_events
            self.connection_event.wait()
            if self.exit_event.is_set(): return

            self.log(f"'{self.name}' connected to root predecessors {list(self.predecessors_events.keys())}", level='INFO')

        while self.exit_event.is_set() is False:
            self.status = Status.QUEUED
            self.wait_for_predecessors()
            
            if self.exit_event.is_set(): return
            self.status = Status.PREPARATION
            self.before_execution()

            if self.exit_event.is_set(): return

            ret = None
            try:
                if self.exit_event.is_set(): return
                self.status = Status.EXECUTING
                ret = await self.execute()
                
                if self.exit_event.is_set(): return
                
                if ret:
                    self.status = Status.COMPLETE
                    self.on_success()
                else:
                    self.status = Status.FAILURE
                    self.on_failure()

            except Exception as e:
                if self.exit_event.is_set(): return
                self.log(f"Error executing action node '{self.name}': {traceback.format_exc()}", level="ERROR")
                self.status = Status.ERROR
                self.on_error(e)

            finally:
                if self.exit_event.is_set(): return
                self.status = Status.CLEANUP
                self.after_execution()

            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS if ret else Result.FAILURE)

            # checking for successors signals before signalling predecessors will 
            # ensure all action nodes have finished using the resource for current run
            if self.exit_event.is_set(): return
            self.wait_for_successors()

            if self.exit_event.is_set(): return
            await self.signal_predecessors(Result.SUCCESS if ret else Result.FAILURE)