# Welcome to the Anacostia Pipeline!


Requirements: Python 3.11 or greater

How to install the pipeline:

1. Create the python virtual env (Linux/Mac)
```
python3 -m venv venv
source venv/bin/activate
```
Create the python virtual env (Windows)
```
python -m venv venv
venv\Scripts\activate
```
2. pip install package locally
```
cd anacostia
pip install -e .
```
3. pip install packages for running tests
```
pip install requirements-test.txt
```
4. Run test
```
cd tests
python3 graph_test.py
```

Anacostia Pipeline Modules:
1. Engine - defines the Anacostia Pipeline
    - base.py:
        - ```BaseNode```: defines methods for implementing the node lifecycles.
        - ```BaseMetadataStoreNode```: defines the node lifecycle for metadata store nodes.
        - ```BaseResourceNode```: defines the node lifecycle for resource nodes.
        - ```BaseActionNode```: defines the node lifecycle for action nodes.
    - pipeline.py: contains functions on how a pipeline is setup, launched, and terminated.
    - utils.py:
        - ```Signal```: defines the information nodes send to each other to trigger execution.
        - ```SignalTable```: a thread-safe dictionary base node classes use to record signals recieved from other nodes.
    - constants.py: defines the ```Status```, ```Work```, and ```Result``` flags. 
2. Metadata - defines the basic metadata stores that users have access to right away:
    - sql_metadata_store.py: a simple metadata store node implemented using SQLite.
3. Resource - defines the basic anacostia resources that users have access to right away:
    - filesystem_store.py: monitoring and non-monitoring resource nodes
4. Tests - where all the tests for pipeline and nodes reside
