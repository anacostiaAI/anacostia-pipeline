import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import shutil
import logging

from anacostia_pipeline.nodes.resources.s3.node import S3Node
from anacostia_pipeline.nodes.metadata.sqlite.node import SqliteMetadataStoreNode
from anacostia_pipeline.nodes.actions.node import BaseActionNode

from anacostia_pipeline.pipelines.root.pipeline import RootPipeline
from anacostia_pipeline.pipelines.root.app import RootPipelineApp

load_dotenv()


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_URL = os.getenv("AWS_BUCKET_URL")


s3_tests_path = "./testing_artifacts/s3"
if os.path.exists(s3_tests_path) is True:
    shutil.rmtree(s3_tests_path)
os.makedirs(s3_tests_path)

log_path = f"{s3_tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


path = f"{s3_tests_path}/s3_test"
metadata_store_path = f"{path}/metadata_store"

metadata_store = SqliteMetadataStoreNode("metadata_store", uri=f"{metadata_store_path}/metadata.db")
s3_store = S3Node("s3_store", metadata_store, AWS_S3_BUCKET_NAME, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, monitoring=True)

class S3ActionNode(BaseActionNode):
    def __init__(self, name: str, s3_store: S3Node, predecessors: list, loggers=None) -> None:
        super().__init__(name, predecessors, loggers)
        self.s3_store = s3_store

    def execute(self) -> bool:
        self.log(f"Executing action node '{self.name}'")
        return True

s3_action = S3ActionNode("s3_action", s3_store, predecessors=[s3_store])


pipeline = RootPipeline(
    nodes=[metadata_store, s3_store, s3_action], 
    loggers=logger
)

if __name__ == "__main__":
    webserver = RootPipelineApp(name="test_pipeline", pipeline=pipeline, host="127.0.0.1", port=8000, logger=logger)
    webserver.run()

'''
def connect_to_s3(bucket_name, region_name=None, aws_access_key_id=None, aws_secret_access_key=None):
    """
    Connect to an S3 bucket and return the bucket resource.
    
    Parameters:
    bucket_name (str): Name of the S3 bucket to connect to
    region_name (str, optional): AWS region name (e.g., 'us-west-2')
    aws_access_key_id (str, optional): AWS access key ID
    aws_secret_access_key (str, optional): AWS secret access key
    
    Returns:
    tuple: (success (bool), bucket_resource or error_message (str))
    """
    try:
        # Create an S3 resource
        s3 = boto3.resource(
            's3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        # Check if bucket exists and is accessible
        bucket = s3.Bucket(bucket_name)

        # Try to access bucket properties to test connection
        bucket.creation_date
        
        print(f"Successfully connected to S3 bucket: {bucket_name}")
        return True, bucket
    
    except ClientError as e:
        error_message = f"Error connecting to S3 bucket: {e}"
        print(error_message)
        return False, error_message

# Example usage
if __name__ == "__main__":
    # Option 1: Connect using AWS credentials from environment variables or ~/.aws/credentials
    success, result = connect_to_s3(AWS_S3_BUCKET_NAME, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    
    if success:
        # Now you can use the bucket
        bucket = result
        
        # Example: List objects in the bucket
        print("Objects in the bucket:")
        for obj in bucket.objects.limit(10):  # Limit to 10 objects
            print(f" - {obj.key} ({obj.size} bytes)")
    
    s3 = boto3.client('s3')
    response = s3.list_buckets()
'''