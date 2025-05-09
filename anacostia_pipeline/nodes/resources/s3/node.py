from threading import Thread
import traceback
import boto3
from typing import List

from anacostia_pipeline.nodes.resources.node import BaseResourceNode
from botocore.exceptions import ClientError



class S3Node(BaseResourceNode):
    def __init__(self, 
        name: str, 
        metadata_store,
        bucket_name: str, 
        region_name: str = None, 
        aws_access_key_id: str = None, 
        aws_secret_access_key: str = None, 
        remote_predecessors: List[str] = None,
        remote_successors: List[str] = None,
        client_url: str = None,
        wait_for_connection: bool = False,
        loggers = None, 
        monitoring = True
    ) -> None:
        super().__init__(
            name, 
            resource_path=None, 
            metadata_store=metadata_store, 
            remote_predecessors=remote_predecessors,
            remote_successors=remote_successors,
            wait_for_connection=wait_for_connection,
            client_url=client_url,
            loggers=loggers, 
            monitoring=monitoring
        )

        self.bucket_name = bucket_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        try:
            # Create an S3 resource
            s3 = boto3.resource(
                's3',
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            
            # Check if bucket exists and is accessible
            self.bucket = s3.Bucket(bucket_name)

            print(f"Successfully connected to S3 bucket: {bucket_name}")
        
        except ClientError as e:
            error_message = f"Error connecting to S3 bucket: {e}"
            print(error_message)
            return False, error_message
    
    def record_new(self, obj_key: str) -> None:
        self.metadata_store.create_entry(self.name, filepath=obj_key, state="new")

    def record_current(self):
        self.metadata_store.create_entry(self.name, filepath=self.bucket_name, state="current", run_id=self.metadata_store.get_run_id())

    def start_monitoring(self) -> None:

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'")
            while self.exit_event.is_set() is False:
                for obj in self.bucket.objects.limit(100):  # Limit to 10 objects
                    if self.metadata_store.entry_exists(self.name, obj.key) is False:
                        self.record_new(obj.key)
                        self.log(f"'{self.name}' detected object: {obj.key}")
    
                if self.exit_event.is_set() is True: break
                try:
                    self.custom_trigger()
                
                except NotImplementedError:
                    self.base_trigger()

                except Exception as e:
                        self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}")
    
        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func)
        self.observer_thread.start()

    def custom_trigger(self) -> bool:
        """
        Override to implement your custom triggering logic. If the custom_trigger method is not implemented, the base_trigger method will be called.
        """
        raise NotImplementedError
    
    def base_trigger(self) -> None:
        """
        The default trigger for the FilesystemStoreNode. 
        base_trigger checks if there are any new files in the resource directory and triggers the node if there are.
        base_trigger is called when the custom_trigger method is not implemented.
        """

        if self.get_num_artifacts("new") > 0:
            self.trigger()

    def stop_monitoring(self) -> None:
        self.log(f"Beginning teardown for node '{self.name}'")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'")
    
    def get_num_artifacts(self, state: str) -> int:
        return self.metadata_store.get_num_entries(self.name, state)
    