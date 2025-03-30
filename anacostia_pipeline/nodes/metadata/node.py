from typing import List, Union, Dict
from logging import Logger
from threading import Event

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
        caller_url: str = None,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
    
        super().__init__(name, predecessors=None, remote_predecessors=None, remote_successors=remote_successors, caller_url=caller_url, loggers=loggers)
        self.uri = uri
        self.run_id = 0
        self.trigger_event = Event()
    
    def get_run_id(self) -> int:
        return self.run_id

    def add_node(self, node_name: str, node_type: str) -> None:
        raise NotImplementedError
    
    def get_nodes_info(self, node_id: int = None, node_name: str = None) -> List[Dict]:
        pass
    
    def create_entry(self, resource_node_name: str, **kwargs) -> None:
        raise NotImplementedError

    def get_entries(self, resource_node_name: str, state: str) -> List[dict]:
        pass

    def update_entry(self, resource_node_name: str, entry_id: int, **kwargs) -> None:
        pass

    def get_num_entries(self, resource_node_name: str, state: str) -> int:
        pass

    def log_metrics(self, node_name: str, **kwargs) -> None:
        pass
    
    def log_params(self, node_name: str, **kwargs) -> None:
        pass

    def set_tags(self, node_name: str, **kwargs) -> None:
        pass

    def get_metrics(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        pass

    def get_params(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        pass

    def get_tags(self, node_name: str = None, run_id: int = None) -> List[Dict]:
        pass

    def start_run(self) -> None:
        """
        Override to specify how to create a run in the metadata store.
        E.g., log the start time of the run, log the parameters of the run, etc. 
        or create a new run in MLFlow, create a new run in Neptune, etc.
        """
        raise NotImplementedError
    
    def end_run(self) -> None:
        """
        Override to specify how to end a run in the metadata store.
        E.g., log the end time of the run, log the metrics of the run, update run_number, etc.
        or end a run in MLFlow, end a run in Neptune, etc.
        """
        raise NotImplementedError
    
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
            # self.log(f"{self.name} waiting for resource nodes to signal they are ready", level='INFO')
            self.wait_for_successors()

            # wait for all metrics to meet trigger conditions
            # self.log(f"{self.name} waiting for metrics to meet trigger conditions", level='INFO')
            self.status = Status.WAITING_METRICS
            self.trigger_event.wait()
            
            if self.exit_event.is_set(): return
            
            self.trigger_event.clear()
            self.status = Status.TRIGGERED

            # creating a new run
            # self.log(f"{self.name} creating a run {self.run_id}", level='INFO')
            if self.exit_event.is_set(): return
            self.start_run()

            # signal to all successors that the run has been created; i.e., begin pipeline execution
            # self.log(f"{self.name} signaling successors that the run has been created", level='INFO')
            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS)

            # waiting for all resource nodes to signal they are done using the current state
            # self.log(f"{self.name} waiting for resource nodes to signal they are done using the current state", level='INFO')
            if self.exit_event.is_set(): return
            self.wait_for_successors()
            
            # ending the run
            # self.log(f"{self.name} ending run {self.run_id}", level='INFO')
            if self.exit_event.is_set(): return
            self.end_run()

            self.run_id += 1
            
            # signal to all successors that the run has ended; i.e., end pipeline execution
            # self.log(f"{self.name} signaling successors that the run has ended", level='INFO')
            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS)