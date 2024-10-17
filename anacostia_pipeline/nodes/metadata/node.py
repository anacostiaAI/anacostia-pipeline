from typing import List, Union
from logging import Logger
from threading import RLock
from functools import wraps

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.utils.constants import Result, Work



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
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
    
        super().__init__(name, predecessors=None, loggers=loggers)
        self.uri = uri
        self.run_id = 0
        self.resource_lock = RLock()
    
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
    def create_resource_tracker(self, resource_node) -> None:
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
    def add_run_id(self) -> None:
        """
        Override to specify how to add the run_id to the resource metadata in the metadata store.
        """
        raise NotImplementedError
    
    @metadata_accessor
    def add_end_time(self) -> None:
        """
        Override to specify how to add the end_time to the resource metadata in the metadata store.
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
    
    def run(self) -> None:
        while self.exit_event.is_set() is False:

            # waiting for all resource nodes to signal their resources are ready to be used
            self.wait_for_successors()

            # creating a new run
            if self.exit_event.is_set(): break
            self.work_list.append(Work.STARTING_RUN)
            self.start_run()
            self.add_run_id()
            self.work_list.remove(Work.STARTING_RUN)

            # signal to all successors that the run has been created; i.e., begin pipeline execution
            if self.exit_event.is_set(): break
            self.signal_successors(Result.SUCCESS)

            # waiting for all resource nodes to signal they are done using the current state
            if self.exit_event.is_set(): break
            self.wait_for_successors()
            
            # ending the run
            if self.exit_event.is_set(): break
            self.work_list.append(Work.ENDING_RUN)
            self.add_end_time()
            self.end_run()
            self.work_list.remove(Work.ENDING_RUN)

            self.run_id += 1
            
            if self.exit_event.is_set(): break
            self.signal_successors(Result.SUCCESS)