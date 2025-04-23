from typing import List, Union, Dict, Any
from logging import Logger
import threading
from abc import ABC, abstractmethod

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.rpc import BaseMetadataRPCCaller
from anacostia_pipeline.utils.constants import Result, Status



class EntryNotFoundError(Exception):
    """Raised when an entry with specified ID is not found"""
    pass



class BaseResourceNode(BaseNode, ABC):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode | BaseMetadataRPCCaller,
        remote_predecessors: List[str] = None, 
        remote_successors: List[str] = None,
        wait_for_connection: bool = False,
        caller_url: str = None,
        loggers: Union[Logger, List[Logger]] = None, 
        monitoring: bool = True
    ) -> None:
        
        super().__init__(
            name, 
            predecessors=[metadata_store], 
            remote_predecessors=remote_predecessors, 
            remote_successors=remote_successors, 
            wait_for_connection=wait_for_connection,
            caller_url=caller_url,
            loggers=loggers
        )

        self.resource_path = resource_path
        self.monitoring = monitoring
        self.metadata_store = metadata_store
        self.resource_event = threading.Event()

    @abstractmethod
    def start_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    @abstractmethod
    def stop_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    @abstractmethod
    def save_artifact(self, *args, **kwargs):
        """Override to specify how the artifact is saved."""
        pass

    @abstractmethod
    def load_artifact(self, *args, **kwargs) -> Any:
        """Override to specify how the artifact is loaded."""
        pass

    @abstractmethod
    async def resource_trigger(self) -> None:
        """Override to specify how the resource is triggered."""
        pass

    async def record_new(self, filepath: str) -> None:
        """
        Record a new artifact in the metadata store.

        Args:
            filepath: The path to the artifact file
        
        Raises:
            TypeError: If the metadata store is not of the expected type
        """

        if isinstance(self.metadata_store, BaseMetadataStoreNode):
            self.metadata_store.create_entry(self.name, filepath=filepath, state="new")

        elif isinstance(self.metadata_store, BaseMetadataRPCCaller):
            await self.metadata_store.create_entry(self.name, filepath=filepath, state="new")
        
        else:
            raise TypeError("metadata_store must be of type BaseMetadataStoreNode or BaseMetadataRPCCaller")

    def record_current(self, filepath: str) -> None:
        self.metadata_store.create_entry(self.name, filepath=filepath, state="current", run_id=self.metadata_store.get_run_id())
    
    def get_num_artifacts(self, state: str) -> int:
        return self.metadata_store.get_num_entries(self.name, state)
    
    def get_artifact(self, id: int) -> Dict:
        """
        Get artifact entry by ID.

        Args:
            id: The ID of the artifact to retrieve
            
        Returns:
            Dict: The artifact entry
            
        Raises:
            EntryNotFoundError: If no entry exists with the specified ID
        """

        entries = self.metadata_store.get_entries(resource_node_name=self.name)
        for entry in entries:
            if entry["id"] == id:
                return entry
        raise EntryNotFoundError(f"No entry found with id: {id}")
    
    def list_artifacts(self, state: str) -> List[str]:
        """
        List all artifacts in the specified state.
        Args:
            state: The state of the artifacts to list (e.g., "new", "current", "old")
        Returns:
            List[str]: A list of file paths of the artifacts in the specified state
        """

        entries = self.metadata_store.get_entries(self.name, state)
        return [entry["location"] for entry in entries]

    def exit(self):
        # call the parent class exit method first to set exit_event, pause_event, all predecessor events, and all successor events.
        super().exit()
        
        # set custom events like resource_event and implement custom exit logic after calling the parent class exit method
        if self.monitoring is True:
            self.stop_monitoring()
        
        self.resource_event.set()
    
    async def trigger(self, message: str = None) -> None:
        if self.resource_event.is_set() is False:
            
            # Note: log the trigger first before setting the event or there will be a race condition
            if message is not None:
                if isinstance(self.metadata_store, BaseMetadataStoreNode):
                    self.metadata_store.log_trigger(node_name=self.name, message=message)
                
                elif isinstance(self.metadata_store, BaseMetadataRPCCaller):
                    await self.metadata_store.log_trigger(node_name=self.name, message=message)
                
                else:
                    raise TypeError("metadata_store must be of type BaseMetadataStoreNode or BaseMetadataRPCCaller")
            
            self.resource_event.set()

    async def run_async(self) -> None:
        # if the node is not monitoring the resource, then we don't need to start the observer / monitoring thread
        if self.monitoring is True:
            self.start_monitoring()

        while self.exit_event.is_set() is False:
            
            # if the node is not monitoring the resource, then we don't need to check for new resources
            # otherwise, we check for new resources and set the resource_event if there are new resources
            if self.monitoring is True:
                # self.log(f"{self.name} checking for new resources", level='INFO')
                self.status = Status.WAITING_RESOURCE
                self.resource_event.wait()

                if self.exit_event.is_set(): return

                self.resource_event.clear()
                self.status = Status.TRIGGERED
                
            # signal to metadata store node that the resource is ready to be used for the next run
            # i.e., tell the metadata store to create and start the next run
            # e.g., there is enough new data to trigger the next run
            # self.log(f"{self.name} signaling metadata store that the resource is ready to be used", level='INFO')
            if self.exit_event.is_set(): return
            await self.signal_predecessors(Result.SUCCESS)

            # wait for metadata store node to finish creating the run 
            # self.log(f"{self.name} waiting for metadata store to finish creating the run", level='INFO')
            if self.exit_event.is_set(): return
            self.wait_for_predecessors()

            # signalling to all successors that the resource is ready to be used for the current run
            # self.log(f"{self.name} signaling successors that the resource is ready to be used", level='INFO')
            if self.exit_event.is_set(): return
            await self.signal_successors(Result.SUCCESS)

            # waiting for all successors to finish using the the resource for the current run
            # self.log(f"{self.name} waiting for successors to finish using the resource", level='INFO')
            if self.exit_event.is_set(): return
            self.wait_for_successors()

            # signal the metadata store node that the action nodes have finish using the resource for the current run
            # self.log(f"{self.name} signaling metadata store that the action nodes have finished using the resource", level='INFO')
            if self.exit_event.is_set(): return
            await self.signal_predecessors(Result.SUCCESS)
            
            # wait for acknowledgement from metadata store node that the run has been ended
            # self.log(f"{self.name} waiting for metadata store to acknowledge that the run has ended", level='INFO')
            if self.exit_event.is_set(): return
            self.wait_for_predecessors()