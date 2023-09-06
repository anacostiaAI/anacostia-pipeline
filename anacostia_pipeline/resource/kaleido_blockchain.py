from anacostia_pipeline.engine.node import ResourceNode
from typing import Any
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os
import base64


# a few things to note:
# 1. we are using the ResourceNode class as a base class for our node:
#    - resource nodes act as a way to procure and set up resources. this includes: 
#       - the resource should be able to download the tool, spin it up, and handle the configuration on behalf of the user; 
#       - e.g., the setup() method downloads the MLflow docker image, spin up the container, 
#         and configure all the right ports and volumes for the container.
#    - resource nodes provide a way to continously monitor the state of the resource. this includes: 
#       - monitor for incoming data
#       - moving data from new->current and current->old
#    - resource nodes act as a common API for users to interact with the underlying resource.
#       - provide functions that allow users to read data from and save data to the resource
#       - Note: users will also use these functions to implement the logic for other nodes
#       - the phrase “common API” implies that the API for resource nodes for two different resources that provide the same function 
#         should be similar (not exact, but as similar as possible). 
#       - e.g. suppose we have our own AnacostiaFeatureStoreNode that implements functions for interacting with 
#         our own Anacostia Feature Store; if the user wants to swap out our Anacostia Feature Store 
#         with a different feature store like FEAST, then they just have to swap out the AnacostiaFeatureStoreNode for a FEASTFeatureStoreNode 
#         without having to make many changes because both the AnacostiaFeatureStoreNode and the FEASTFeatureStoreNode have a 
#         get_feature_vectors() method. 
# 2. resource nodes should not take any other nodes as input; resource nodes do not listen to any other nodes, 
#    they are the base of the pipeline; i.e., resource nodes have no predecessors. 
# 3. there are three decorators that can be used to define a function:
#    - @resource_accessor: a function that accesses the underlying resource.
#    - @externally_accessible: a function that can be called by the user.
#    - @await_references: a function that is called when the node is ready to update its state.
#    - we use @resource_accessor by itself to decorate functions that will only run inside this class 
#      (e.g., like the setup() and on_modified() functions in DataStoreNode or a helper method that you created for this class).
#    - we use @externally_accessible followed by @resource_accessor to decorate functions that can be called by the user,
#      or functions that can be overridden by the user 
#      (e.g., like the trigger_condition() method and the user_overridable_example() method below).
#    - we use @await_references followed by @resource_accessor to decorate the function that is responsible for the state change
#      (e.g., like the execute() method below).
#    - when decorating a function with more than one decorator, the @ResourceNode.resource_accessor must always be the second decorator
# 4. there are three states: current, old, and new.
#    - new: the data that was recently added to the pipeline.
#    - current: the data that is currently being used by the pipeline.
#    - old: the data that was previously used by the pipeline.
class KaleidoBlockchainNode(ResourceNode):
    def __init__(self, name: str, url: str, username: str = None, password: str = None, env_path: str = None) -> None:
        self.url = url
        
        if env_path is not None:
            load_dotenv(env_path)
            self.username = os.getenv("USERNAME")
            self.password = os.getenv("PASSWORD")
        else:
            if username is None or password is None:
                raise Exception("Either env_path or username and password must be provided")
            self.username = username
            self.password = password

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {self.credentials}",
            "Content-Type": "application/json"
        }
    
        super().__init__(name, "kaleido_blockchain")

    @ResourceNode.resource_accessor
    def setup(self) -> None:
        # TODO task #1: implement setup() method
        # figure out a way to get the list of current on-chain events (or whatever information you deem necessary) 
        # and store them in a local database or file (see data_store.py and data_store.json for an example);
        # if you choose to go with the file route, name the file kaleido_blockchain.json.
        # if there are currently no events on-chain, then you can just store an empty list in the json file or set up an empty database.
        # if there are currently events on-chain, then you should store the events in the json file or database 
        # and set the state of the event to "current" (in the case of using a json file, see data_store.json; 
        # in the case of using a database, create an attribute in the row to store the state 
        # and store the value you designated as "current" in that attribute for that row).
        # i recommend going with the json file route as i've already done it and i can more easily guide you through that process.
        pass

    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def trigger_condition(self) -> bool:
        # this is the default implementation. we trigger the next node as soon as we see a new event in the blockchain.
        # this method is for the user to override; do not touch this method.  
        return True
    
    @ResourceNode.resource_accessor
    def pre_trigger(self) -> bool:
        # TODO task #2: use this method to implement a way to monitor the database for incoming data
        # in this method, you should check the database for new events and update the state of the events accordingly.
        # the state of all new events are recorded as "new" in the database.

        # possible implementation:
        #   - ping the api to check for new events. 
        #   - if there are new events, then:
        #       - record the events in the database/json file
        #       - set the state of the recorded entry to "new"
        #       - set state_changed to True.
        #   - if there are no new events, then set state_changed to False.
        # do not put this implementation inside a while loop, the node.run() method will call this method continuously in its while loop.
        state_changed = False
        if state_changed is True:
            if self.trigger_condition() is True:
                self.trigger()
            return True
        else:
            return False
    
    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def broadcast_message(self, message, metadata=None):
        # this is an example of an API method that can be called by the user.
        # note that we use the @externally_accessible decorator followed by the @resource_accessor decorator to decorate this method.
        # this ensures that when the user calls can call this method without having to worry about Anacostia's synchronization requirements.

        # i don't know what we intend to do with the metadata parameter, so i've commented it out for now.
        # if we do decide to use it, then we should figure out a way to make sure that the metadata is in the correct format.
        # if we don't use it, then we can just delete the metadata parameter.

        # TODO task #4: implement methods to interact with the API 
        #   - subtask: look through the Kaleido API documentation and figure out which API endpoints we can combine together 
        #     (we don't want to create a method for every single API endpoint, that would be too much for the user to digest).
        #   - subtask: design the methods we need to implement to interact with the combined API endpoints.
        # Note: methods should allow users to read and write data to the blockchain via the API endpoints.
        payload = {
            "data": [
                {
                    "value": message,
                    # "metadata": metadata
                }
            ]
        }
        response = requests.post(f"{self.url}/messages/broadcast", json=payload, headers=self.headers)
        return response.json()
    
    @ResourceNode.exeternally_accessible
    @ResourceNode.resource_accessor
    def user_overridable_example(self, filepath: str) -> Any:
        # this is an example of a function that can be overridden by the user.
        # in your implementation, if you don't need to provide an overridable method like this, then you can delete this method.
        # note that we use the @externally_accessible decorator followed by the @resource_accessor decorator to decorate this method.
        # this ensures that the user's implementation of this method will obey all Anacostia's synchronization requirements.
        raise NotImplementedError

    @ResourceNode.await_references
    @ResourceNode.resource_accessor
    def execute(self):
        # TODO task #3: use this method to update the state of the resource 
        # (i.e., updating database rows or json file entries from new->current and current->old).
        return True

    def on_exit(self) -> None:
        # TODO: if necessary, implement this method to close the API connection when the node is terminated here
        pass
