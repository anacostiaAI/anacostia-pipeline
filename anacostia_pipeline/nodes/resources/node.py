from typing import List, Union, Dict, Any
from logging import Logger
import threading
from abc import ABC, abstractmethod

import httpx

from anacostia_pipeline.nodes.node import BaseNode
from anacostia_pipeline.nodes.metadata.node import BaseMetadataStoreNode
from anacostia_pipeline.nodes.metadata.api import BaseMetadataStoreClient
from anacostia_pipeline.utils.constants import Result, Status
from anacostia_pipeline.nodes.utils import NodeModel



class EntryNotFoundError(Exception):
    """Raised when an entry with specified ID is not found"""
    pass



class BaseResourceNode(BaseNode, ABC):
    def __init__(
        self, 
        name: str, 
        resource_path: str, 
        metadata_store: BaseMetadataStoreNode = None,
        metadata_store_client: BaseMetadataStoreClient = None,
        remote_predecessors: List[str] = None, 
        remote_successors: List[str] = None,
        wait_for_connection: bool = False,
        client_url: str = None,
        loggers: Union[Logger, List[Logger]] = None, 
        monitoring: bool = True
    ) -> None:
        
        super().__init__(
            name, 
            predecessors = [metadata_store] if metadata_store else None, 
            remote_predecessors=remote_predecessors, 
            remote_successors=remote_successors, 
            wait_for_connection=wait_for_connection,
            client_url=client_url,
            loggers=loggers
        )

        self.resource_path = resource_path
        self.monitoring = monitoring

        if metadata_store is None and metadata_store_client is None:
            raise ValueError("Either metadata_store or metadata_store_rpc must be provided")

        self.metadata_store = metadata_store
        self.metadata_store_client = metadata_store_client
        self.resource_event = threading.Event()

    def model(self) -> NodeModel:
        return NodeModel(
            name = self.name,
            node_type = type(self).__name__,
            base_type = "BaseResourceNode",
            predecessors = [n.name for n in self.predecessors],
            successors = [n.name for n in self.successors]
        )

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
    async def save_artifact(self, *args, **kwargs):
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

    async def entry_exists(self, filepath: str) -> bool:
        if self.metadata_store is not None:
            return self.metadata_store.entry_exists(self.name, filepath)
        
        if self.connection_event.is_set() is True:
            if self.metadata_store_client is not None:
                try:
                    return await self.metadata_store_client.entry_exists(self.name, filepath)
                except httpx.ConnectError as e:
                    self.log(f"FilesystemStoreNode '{self.name}' is no longer connected", level="ERROR")
                    raise e
                    # if an exception is raised here, it means the node is no longer connected to the metadata store on the root pipeline
            

    async def record_new(self, filepath: str, hash: str, hash_algorithm: str) -> None:
        """
        Record a new artifact in the metadata store.

        Args:
            filepath: The path to the artifact file
        """

        if self.metadata_store is not None:
            self.metadata_store.create_entry(self.name, filepath=filepath, state="new", hash=hash, hash_algorithm=hash_algorithm)

        if self.connection_event.is_set() is True:
            if self.metadata_store_client is not None:
                try:
                    await self.metadata_store_client.create_entry(self.name, filepath=filepath, state="new", hash=hash, hash_algorithm=hash_algorithm)
                except httpx.ConnectError as e:
                    self.log(f"FilesystemStoreNode '{self.name}' is no longer connected", level="ERROR")
                    raise e
                except httpx.HTTPStatusError as e:
                    self.log(f"HTTP error: {e}", level="ERROR")
                    raise e
                except Exception as e:
                    self.log(f"Unexpected error: {e}", level="ERROR")
                    raise e
                                
        
    async def record_current(self, filepath: str, hash: str, hash_algorithm: str) -> None:
        """
        Record an artifact produced in the current run metadata store.

        Args:
            filepath: The path to the artifact file
        """

        if self.metadata_store is not None:
            self.metadata_store.create_entry(self.name, filepath=filepath, state="current", hash=hash, hash_algorithm=hash_algorithm)

        if self.connection_event.is_set() is True:
            if self.metadata_store_client is not None:
                try:
                    await self.metadata_store_client.create_entry(self.name, filepath=filepath, state="current", hash=hash, hash_algorithm=hash_algorithm)
                except httpx.ConnectError as e:
                    self.log(f"FilesystemStoreNode '{self.name}' is no longer connected", level="ERROR")
                    raise e
                except httpx.HTTPStatusError as e:
                    self.log(f"HTTP error: {e}", level="ERROR")
                    raise e
                except Exception as e:
                    self.log(f"Unexpected error: {e}", level="ERROR")
                    raise e
    
    async def tag_artifact(self, filepath: str, **kwargs) -> None:
        """
        Tag an artifact in the metadata store.
        Args:
            filepath: The path to the artifact file
            **kwargs: tags to pass to the metadata store (example call: tag_artifact(node_name, location, customer="customer_name", tag="tag_name"))
        """ 

        if self.metadata_store is not None:
            self.metadata_store.tag_artifact(node_name=self.name, location=filepath, **kwargs)
        
        # currently, the metadata store client does not support tagging artifacts so it doesn't work, 
        # but we keep this method for consistency with the metadata store node interface
        if self.connection_event.is_set() is True:
            if self.metadata_store_client is not None:
                try:
                    await self.metadata_store_client.tag_artifact(node_name=self.name, location=filepath, **kwargs)
                except httpx.ConnectError as e:
                    self.log(f"Resource node '{self.name}' is no longer connected", level="ERROR")
                    raise e
                except httpx.HTTPStatusError as e:
                    self.log(f"HTTP error: {e}", level="ERROR")
                    raise e
                except Exception as e:
                    self.log(f"Unexpected error: {e}", level="ERROR")
                    raise e
        
    async def add_artifact(
        self, filepath: str, hash: str, hash_algorithm: str, state: str = "new", run_id: int = None, file_size: int = None, content_type: str = None
    ) -> None:
        """
        Record an artifact produced in the metadata store.

        Args:
            filepath: The path to the artifact file
        """

        if self.metadata_store is not None:
            self.metadata_store.create_entry(
                self.name, filepath=filepath, hash=hash, hash_algorithm=hash_algorithm, state=state, run_id=run_id, file_size=file_size, content_type=content_type
            )

        if self.connection_event.is_set() is True:
            if self.metadata_store_client is not None:
                try:
                    await self.metadata_store_client.create_entry(
                        self.name, filepath=filepath, hash=hash, hash_algorithm=hash_algorithm, state=state, run_id=run_id, file_size=file_size, content_type=content_type
                    )
                except httpx.ConnectError as e:
                    self.log(f"Resource node '{self.name}' is no longer connected", level="ERROR")
                    raise e
                except httpx.HTTPStatusError as e:
                    self.log(f"HTTP error: {e}", level="ERROR")
                    raise e
                except Exception as e:
                    self.log(f"Unexpected error: {e}", level="ERROR")
                    raise e

    async def get_num_artifacts(self, state: str) -> int:
        """
        Get the number of artifacts in the specified state.
        
        Args:
            state: The state of the artifacts to count (e.g., "new", "current", "old")
        
        Returns:
            int: The number of artifacts in the specified state
        """

        if self.metadata_store is not None:
            return self.metadata_store.get_num_entries(self.name, state)
        
        if self.metadata_store_client is not None:
            return await self.metadata_store_client.get_num_entries(self.name, state)

    def get_artifact(self, id: int) -> Dict:
        """
        Get artifact entry by ID.
        
        Args:
            id: The ID of the artifact to retrieve
        
        Returns:
            Dict: The artifact entry
        """

        entries = self.metadata_store.get_entries(resource_node_name=self.name)
        for entry in entries:
            if entry["id"] == id:
                return entry
        raise EntryNotFoundError(f"No entry found with id: {id}")
    
    async def list_artifacts(self, state: str) -> List[str]:
        """
        List all artifacts in the specified state.
        Args:
            state: The state of the artifacts to list (e.g., "new", "current", "old")
        Returns:
            List[str]: A list of file paths of the artifacts in the specified state
        """

        if self.metadata_store is not None:
            entries = self.metadata_store.get_entries(self.name, state)
        
        if self.metadata_store_client is not None:
            entries = await self.metadata_store_client.get_entries(self.name, state)

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
                if self.metadata_store is not None:
                    self.metadata_store.log_trigger(node_name=self.name, message=message)
                
                if self.connection_event.is_set() is True:
                    if self.metadata_store_client is not None:
                        await self.metadata_store_client.log_trigger(node_name=self.name, message=message)
            
            self.resource_event.set()

    async def run_async(self) -> None:
        # if the node is not monitoring the resource, then we don't need to start the observer / monitoring thread
        if self.monitoring is True:
            self.start_monitoring()

        if self.wait_for_connection:
            self.log(f"'{self.name}' waiting for root predecessors to connect", level='INFO')
            
            # this event is set by the LeafPipeline when all root predecessors are connected and after it adds to predecessors_events
            self.connection_event.wait()
            if self.exit_event.is_set(): return

            self.log(f"'{self.name}' connected to root predecessors {list(self.predecessors_events.keys())}", level='INFO')

        if self.metadata_store_client is not None and self.metadata_store is not None and self.wait_for_connection is True:
            entries = self.metadata_store.get_entries(self.name)
            await self.metadata_store_client.merge_artifacts_table(self.name, entries)

        while self.exit_event.is_set() is False:
            
            # if the node is not monitoring the resource, then we don't need to check for new resources
            # otherwise, we check for new resources and set the resource_event if there are new resources
            if self.monitoring is True:
                # self.log(f"{self.name} checking for new resources", level='INFO')
                self.status = Status.WAITING_RESOURCE
                self.resource_event.wait()

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

            # we clear the resource_event after the run has been created so that the trigger won't execute more than once on the same resource
            # this is important so we don't trigger the same run multiple times if the resource is not changing
            if self.monitoring is True:
                self.resource_event.clear()
                self.status = Status.TRIGGERED
                
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