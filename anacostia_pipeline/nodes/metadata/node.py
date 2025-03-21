from typing import List, Union
from logging import Logger
from threading import RLock, Event
from functools import wraps

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.utils.constants import Result, Status



class BaseMetadataStoreNode(BaseNode):
    """
    Base class for metadata store nodes.
    Metadata store nodes are nodes that are used to store metadata about the pipeline; 
    e.g., store information about experiments (start time, end time, metrics, etc.).
    The metadata store node is a special type of resource node that will be the predecessor of all other resource nodes;
    thus, by extension, the metadata store node will always be the root node of the DAG.
    """
    def __init__(
        self,
        name: str,
        uri: str,
        remote_successors: List[str] = None,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
    
        super().__init__(name, predecessors=None, remote_predecessors=None, remote_successors=remote_successors, loggers=loggers)
        self.uri = uri
        self.run_id = 0
        self.resource_lock = RLock()
        self.trigger_event = Event()
    
    def get_run_id(self) -> int:
        return self.run_id

    def metadata_accessor(func):
        @wraps(func)
        def wrapper(self: 'BaseMetadataStoreNode', *args, **kwargs):
            # keep trying to acquire lock until function is finished
            # generally, it is best practice to use lock inside of a while loop to avoid race conditions (recall GMU CS 571)
            while True:
                with self.resource_lock:
                    result = func(self, *args, **kwargs)
                    return result
        return wrapper
    
    @metadata_accessor
    def add_node(self, node) -> None:
        raise NotImplementedError
    
    @metadata_accessor
    def create_entry(self, resource_node, **kwargs) -> None:
        raise NotImplementedError

    @metadata_accessor
    def get_entries(self, resource_node) -> List[dict]:
        pass

    @metadata_accessor
    def update_entry(self, resource_node, entry_id: int, **kwargs) -> None:
        pass

    @metadata_accessor
    def get_num_entries(self, resource_node) -> int:
        pass

    @metadata_accessor
    def log_metrics(self, **kwargs) -> None:
        pass
    
    @metadata_accessor
    def log_params(self, **kwargs) -> None:
        pass

    @metadata_accessor
    def set_tags(self, **kwargs) -> None:
        pass

    @metadata_accessor
    def start_run(self) -> None:
        """
        Override to specify how to create a run in the metadata store.
        E.g., log the start time of the run, log the parameters of the run, etc. 
        or create a new run in MLFlow, create a new run in Neptune, etc.
        """
        raise NotImplementedError
    
    @metadata_accessor
    def end_run(self) -> None:
        """
        Override to specify how to end a run in the metadata store.
        E.g., log the end time of the run, log the metrics of the run, update run_number, etc.
        or end a run in MLFlow, end a run in Neptune, etc.
        """
        raise NotImplementedError
    
    @metadata_accessor
    def entry_exists(self) -> bool:
        raise NotImplementedError
    
    def trigger(self) -> None:
        self.trigger_event.set()

    def start_monitoring(self) -> None:
        """
        Override to specify how to start monitoring the metadata store.
        E.g., start a thread that monitors the metadata store for new entries, etc.
        """
        pass

    def stop_monitoring(self) -> None:
        """
        Override to specify how to stop monitoring the metadata store.
        E.g., stop the thread that monitors the metadata store for new entries, etc.
        """
        pass

    def exit(self):
        # call the parent class exit method first to set exit_event, pause_event, all predecessor events, and all successor events.
        super().exit()
        self.stop_monitoring()    
        self.trigger_event.set()
    
    async def run_async(self) -> None:
        # start monitoring thread for metadata store node
        self.start_monitoring()

        while self.exit_event.is_set() is False:

            # waiting for all resource nodes to signal their resources are ready to be used
            self.wait_for_successors()

            # wait for all metrics to meet trigger conditions
            self.status = Status.WAITING_METRICS
            self.trigger_event.wait()
            
            if self.exit_event.is_set(): return
            
            self.trigger_event.clear()
            self.status = Status.TRIGGERED

            # creating a new run
            if self.exit_event.is_set(): return
            self.start_run()

            # signal to all successors that the run has been created; i.e., begin pipeline execution
            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS)

            # waiting for all resource nodes to signal they are done using the current state
            if self.exit_event.is_set(): return
            self.wait_for_successors()
            
            # ending the run
            if self.exit_event.is_set(): return
            self.end_run()

            self.run_id += 1
            
            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS)