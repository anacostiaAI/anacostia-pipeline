from typing import List, Union
from logging import Logger
from threading import RLock
from functools import wraps
import time
import traceback

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.utils.constants import Result, Work



class BaseResourceNode(BaseNode):
    def __init__(
        self, 
        name: str, resource_path: str, metadata_store,
        loggers: Union[Logger, List[Logger]] = None, monitoring: bool = True
    ) -> None:
        super().__init__(name, predecessors=[metadata_store], loggers=loggers)
        self.resource_path = resource_path
        self.resource_lock = RLock()
        self.monitoring = monitoring
        self.metadata_store = metadata_store

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
    
    @BaseNode.log_exception
    @resource_accessor
    def trigger_condition(self) -> bool:
        return True

    def run(self) -> None:
        # if the node is not monitoring the resource, then we don't need to start the observer / monitoring thread
        if self.monitoring is True:
            self.work_list.append(Work.MONITORING_RESOURCE)
            self.start_monitoring()

        while self.exit_event.is_set() is False:
            # if the node is not monitoring the resource, then we don't need to check for new files
            if self.monitoring is True:
                self.work_list.append(Work.WAITING_RESOURCE)

                while self.exit_event.is_set() is False:
                    try:
                        if self.trigger_condition() is True:
                            break
                        else:
                            time.sleep(0.1)

                    except Exception as e:
                        self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                        continue
                        # Note: we continue here because we want to keep trying to check the resource until it is available
                        # with that said, we should add an option for the user to specify the number of times to try before giving up
                        # and throwing an exception
                        # Note: we also continue because we don't want to stop checking in the case of a corrupted file or something like that. 
                        # We should also think about adding an option for the user to specify what actions to take in the case of an exception,
                        # e.g., send an email to the data science team to let everyone know the resource is corrupted, 
                        # or just not move the file to current.
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
            
