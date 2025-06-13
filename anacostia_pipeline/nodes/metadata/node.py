from typing import List, Union, Dict
from logging import Logger
from threading import Event
from threading import Thread
import traceback
import time

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.utils.constants import Result, Status
from anacostia_pipeline.nodes.utils import NodeModel



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
        client_url: str = None,
        loggers: Union[Logger, List[Logger]] = None
    ) -> None:
    
        super().__init__(name, predecessors=None, remote_predecessors=None, remote_successors=remote_successors, client_url=client_url, loggers=loggers)
        self.uri = uri
        self.run_id = 0
        self.trigger_event = Event()
    
    def model(self) -> NodeModel:
        return NodeModel(
            name = self.name,
            node_type = type(self).__name__,
            base_type = "BaseMetadataStoreNode",
            predecessors = [n.name for n in self.predecessors],
            successors = [n.name for n in self.successors]
        )

    def get_run_id(self) -> int:
        return self.run_id
    
    def get_node_id(self, node_name: str) -> int:
        """
        Override to specify how to get the node ID from the metadata store.
        E.g., get the node ID from MLFlow, get the node ID from Neptune, etc.
        """
        pass

    def node_exists(self, node_name: str) -> bool:
        pass

    def add_node(self, node_name: str, node_type: str, base_type: str) -> None:
        pass
    
    def get_nodes_info(self, node_id: int = None, node_name: str = None) -> List[Dict]:
        pass
    
    def create_entry(self, resource_node_name: str, **kwargs) -> None:
        pass

    def merge_artifacts_table(self, resource_node_name: str, entries: List[Dict]) -> None:
        pass

    def get_entries(self, resource_node_name: str = None, state: str = "all", run_id: int = None) -> List[dict]:
        pass

    def update_entry(self, resource_node_name: str, entry_id: int, **kwargs) -> None:
        pass

    def entry_exists(self, resource_node_name: str, location: str) -> bool:
        pass

    def get_num_entries(self, resource_node_name: str, state: str) -> int:
        pass

    def log_metrics(self, node_name: str, **kwargs) -> None:
        pass
    
    def log_params(self, node_name: str, **kwargs) -> None:
        pass
    
    def tag_artifact(self, node_name: str, location: str, **kwargs) -> None:
        pass

    def get_artifact_tags(self, location: str) -> List[Dict]:
        """
        Override to specify how to get the tags of an artifact from the metadata store.
        E.g., get the tags of an artifact from MLFlow, get the tags of an artifact from Neptune, etc.
        """
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
        pass
    
    def entry_exists(self, resource_node_name: str) -> bool:
        pass
    
    def log_trigger(self, node_name: str, message: str = None) -> None:
        """
        Override to specify how to log a trigger in the metadata store.
        E.g., log the trigger message, the trigger time, the run the trigger triggered, and the node_id of the node with name node_name.
        """
        pass

    def trigger(self, message: str = None) -> None:
        if self.trigger_event.is_set() is False:
            
            # Note: log the trigger first before setting the event or there will be a race condition
            if message is not None:
                self.log_trigger(node_name=self.name, message=message)
            
            self.trigger_event.set()

    def start_monitoring(self) -> None:

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.exit_event.is_set() is False:
                if self.exit_event.is_set() is True: break
                try:
                    self.metadata_store_trigger()

                except Exception as e:
                        self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
                
                # IMPORTANT: sleep for a while before checking again to enable other threads to access the database and to avoid starvation.
                time.sleep(0.1)

        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func)
        self.observer_thread.start()

    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'")
    
    def metadata_store_trigger(self) -> None:
        """
        The default trigger for the SqliteMetadataStoreNode. 
        In the default trigger, the metadata_store_trigger does not check anything in the metadata store, it just simply triggers the pipeline.
        Override metadata_store_trigger in order to implement your own metadata store trigger (e.g., metric-based triggers)
        """
        self.trigger()
    
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
            self.log(f"--------------------------------- {self.name} creating a run {self.run_id}", level='INFO')
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
            self.log(f"--------------------------------- {self.name} ending run {self.run_id}", level='INFO')
            if self.exit_event.is_set(): return
            self.end_run()

            self.run_id += 1
            
            # signal to all successors that the run has ended; i.e., end pipeline execution
            # self.log(f"{self.name} signaling successors that the run has ended", level='INFO')
            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS)