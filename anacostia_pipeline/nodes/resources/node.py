from typing import List, Union
from logging import Logger
import threading

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.utils.constants import Result, Status



class BaseResourceNode(BaseNode):
    def __init__(
        self, 
        name: str, resource_path: str, metadata_store: BaseMetadataStoreNode,
        loggers: Union[Logger, List[Logger]] = None, monitoring: bool = True
    ) -> None:
        super().__init__(name, predecessors=[metadata_store], loggers=loggers)
        self.resource_path = resource_path
        self.monitoring = monitoring
        self.metadata_store = metadata_store
        self.resource_event = threading.Event()

    @BaseNode.log_exception
    def start_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    @BaseNode.log_exception
    def stop_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    @BaseNode.log_exception
    def record_new(self) -> None:
        """
        override to specify how to detect new files and mark the detected files as 'new'
        """
        raise NotImplementedError
    
    @BaseNode.log_exception
    def record_current(self) -> None:
        """
        override to specify how to label newly created files as 'current'
        """
        pass
    
    def exit(self):
        # call the parent class exit method first to set exit_event, pause_event, all predecessor events, and all successor events.
        super().exit()
        
        # set custom events like resource_event and implement custom exit logic after calling the parent class exit method
        if self.monitoring is True:
            self.stop_monitoring()
        
        self.resource_event.set()
    
    def trigger(self) -> None:
        self.resource_event.set()

    async def run_async(self) -> None:
        # if the node is not monitoring the resource, then we don't need to start the observer / monitoring thread
        if self.monitoring is True:
            self.start_monitoring()

        while self.exit_event.is_set() is False:
            
            # if the node is not monitoring the resource, then we don't need to check for new resources
            # otherwise, we check for new resources and set the resource_event if there are new resources
            if self.monitoring is True:
                self.status = Status.WAITING_RESOURCE
                self.resource_event.wait()

                if self.exit_event.is_set(): return

                self.resource_event.clear()
                self.status = Status.TRIGGERED
                
            # signal to metadata store node that the resource is ready to be used for the next run
            # i.e., tell the metadata store to create and start the next run
            # e.g., there is enough new data to trigger the next run
            if self.exit_event.is_set(): return
            await self.signal_predecessors(Result.SUCCESS)

            # wait for metadata store node to finish creating the run 
            if self.exit_event.is_set(): return
            self.wait_for_predecessors()

            # signalling to all successors that the resource is ready to be used for the current run
            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the the resource for the current run
            if self.exit_event.is_set(): return
            self.wait_for_successors()

            # signal the metadata store node that the action nodes have finish using the resource for the current run
            if self.exit_event.is_set(): return
            await self.signal_predecessors(Result.SUCCESS)
            
            # wait for acknowledgement from metadata store node that the run has been ended
            if self.exit_event.is_set(): return
            self.wait_for_predecessors()
            
