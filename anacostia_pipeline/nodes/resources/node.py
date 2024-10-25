from typing import List, Union
from logging import Logger
import threading
from threading import RLock
from functools import wraps

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.utils.constants import Result, Work



class BaseResourceNode(BaseNode):
    def __init__(
        self, 
        name: str, resource_path: str, metadata_store: BaseMetadataStoreNode,
        loggers: Union[Logger, List[Logger]] = None, monitoring: bool = True
    ) -> None:
        super().__init__(name, predecessors=[metadata_store], loggers=loggers)
        self.resource_path = resource_path
        self.resource_lock = RLock()
        self.monitoring = monitoring
        self.metadata_store = metadata_store
        self.resource_event = threading.Event()

    def resource_accessor(func):
        @wraps(func)
        def resource_accessor_wrapper(self: 'BaseResourceNode', *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                with self.resource_lock:
                    result = func(self, *args, **kwargs)
                    return result
        return resource_accessor_wrapper
    
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

    def on_exit(self):
        if self.monitoring is True:
            self.stop_monitoring()
            self.work_list.remove(Work.MONITORING_RESOURCE)

    @BaseNode.log_exception
    @resource_accessor
    def record_new(self) -> None:
        """
        override to specify how to detect new files and mark the detected files as 'new'
        """
        raise NotImplementedError
    
    @BaseNode.log_exception
    @resource_accessor
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
            self.work_list.remove(Work.MONITORING_RESOURCE)
        
        self.resource_event.set()
    
    def trigger(self) -> None:
        self.resource_event.set()

    def run(self) -> None:
        # if the node is not monitoring the resource, then we don't need to start the observer / monitoring thread
        if self.monitoring is True:
            self.work_list.append(Work.MONITORING_RESOURCE)
            self.start_monitoring()

        while self.exit_event.is_set() is False:
            
            # if the node is not monitoring the resource, then we don't need to check for new resources
            # otherwise, we check for new resources and set the resource_event if there are new resources
            if self.monitoring is True:
                self.work_list.append(Work.WAITING_RESOURCE)
                self.resource_event.wait()
                
                if self.exit_event.is_set(): break
                
                self.resource_event.clear()
                self.work_list.remove(Work.WAITING_RESOURCE)
                
            # signal to metadata store node that the resource is ready to be used for the next run
            # i.e., tell the metadata store to create and start the next run
            # e.g., there is enough new data to trigger the next run
            if self.exit_event.is_set(): break
            self.signal_predecessors(Result.SUCCESS)

            # wait for metadata store node to finish creating the run 
            if self.exit_event.is_set(): break
            self.wait_for_predecessors()

            # signalling to all successors that the resource is ready to be used for the current run
            if self.exit_event.is_set(): break
            self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the the resource for the current run
            if self.exit_event.is_set(): break
            self.wait_for_successors()

            # signal the metadata store node that the action nodes have finish using the resource for the current run
            if self.exit_event.is_set(): break
            self.signal_predecessors(Result.SUCCESS)
            
            # wait for acknowledgement from metadata store node that the run has been ended
            if self.exit_event.is_set(): break
            self.wait_for_predecessors()
            
