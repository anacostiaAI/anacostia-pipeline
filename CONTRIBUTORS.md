# Welcome to the Anacostia Pipeline!


### Requirements: 
Python 3.11 or greater, if you need to update Python, here are the steps:

Updating Python on Mac
Follow these steps to update your Python installation to the latest version on Mac:
1. Check Current Python Version
Open a terminal and use the `python3 --version` command to check which Python version you have installed.
For example:
```
python3 --version 
Python 3.7.4
```
2. Install Homebrew. If you don't already have Homebrew installed, you can install it with:
```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
3. Update Homebrew
Before installing Python, update Homebrew's package index with:
```brew update```
4. Search for Latest Python
Now search Brew for the latest available Python version:
```brew search python```
Make note of the latest 3.x release.

6. Install Latest Python
Use brew to install the latest Python version:

```brew install python@3.11```

6. Remove Old Python Version
Remove the outdated Python version, replacing 3.7 with your current outdated version:

```sudo rm -rf /Library/Frameworks/Python.framework/Versions/3.7```

7. Set New Python as Default
Set the new Python as the default by adding it to your PATH in bash profile:

```
echo "alias python=/usr/local/bin/python3" >> ~/.zshrc
source ~/.zshrc
```
8. Confirm New Version
```python --version```

### How to install the pipeline:

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
3. Install test dependencies
```
pip install requirements-test.txt
```
4. Run test
```
cd tests
python3 graph_test.py
```

### Packaging and publishing to PyPI:
1. Build the package:
```
cd anacostia-pipeline
python3 -m build
```
you should see a folder created call `dist/`.

2. If rebuilding the package, do the following:
    a. change the version number.
    b. delete the `dist/` folder (if present).

3. upload package onto PyPI
```
twine upload dist/*
```
For additional directions regarding adding a `~/.pypirc` file, see https://github.com/mdo6180/code-records/blob/main/python/packaging/guide.txt

### Anacostia Project Structure:
```
anacostia_pipeline/
    ├── nodes/
    |   ├── node.py - contains BaseNode
    |   ├── gui.py - contains BaseApp
    |   ├── fragments.py - contains html fragments for BaseApp
    |   ├── actions/
    |   |   └── node.py - contains BaseActionNode (inherits BaseNode)
    |   ├── metadata/
    |   |   ├── node.py - contains BaseMetadataStoreNode (inherits BaseNode)
    |   |   ├── rpc.py - contains BaseMetadataRPCCaller and BaseMetadataRPCCaller
    |   |   ├── sql/
    |   |   |   ├── node.py - contains BaseSQLMetadataStoreNode (inherits BaseMetadataStoreNode) 
    |   |   |   ├── gui.py - contains SQLMetadataStoreGUI (inherits from BaseGUI) 
    |   |   |   ├── fragments.py - contains html fragments for the metadata store GUI.
    |   |   |   ├── rpc.py - contains RPC caller and callee that enables nodes to interact with and send data to each other over the network. 
    |   |   |   ├── models.py - contains models to create tables in the metadata store
    |   |   |   └── sqlite/
    |   |   |       └── contains SqliteMetadataStoreNode (inherits BaseMetadataStoreNode)
    |   |   └── nosql/
    |   |       └── node.py - contains BaseNoSQLMetadataStoreNode (inherits BaseMetadataStoreNode) 
    |   ├── resources/
    |   |   ├── node.py - contains BaseResourceNode (inherits BaseNode)
    |   |   ├── rpc.py - contains BaseResourceRPCCaller and BaseResourceRPCCallee
    |   |   └── filesystem/
    |   |       ├── node.py - contains FilesystemStoreNode (inherits BaseResourceNode)
    |   |       ├── gui.py - contains FilesystemStoreApp (inherits BaseApp)
    |   |       └── fragments.py - contains html fragments for FilesystemStoreApp
    ├── pipelines/ 
    |   ├── leaf/
    |   |   ├── pipeline.py - contains LeafPipeline
    |   |   └── server.py - contains LeafPipelineApp
    |   └── root/
    |       ├── pipeline.py - contains RootPipeline
    |       └── server.py - contains RootPipelineApp
    ├── services/ 
    |   ├── leaf/
    |   |   └── gui.py - contains LeafServiceApp
    |   └── root/
    |       └── gui.py - contains RootServiceApp
    ├── static/ - contains css, img, and js used by the project's html fragments
    ├── utils/ 
    |   ├── constants.py - contains enums and constants used in the project
    |   └── sse.py - contains functions for working with server-sent events
    ├── distributed_tests/ 
    └── multithreaded_tests/ 
```
`anacostia_pipeline` - this is the main package where all the source code for Anacostia is stored.

`anacostia_pipeline/nodes` - this is where all node classes live.

Notice how the `nodes/` package and all of its subpackages have a similar structure with the files:
- `node.py`: contains the code for the actual node itself.
- `gui.py`: contains the code for the FastAPI sub-application that provides a custom GUI for visualizing information pertaining to the node.
- `fragments.py`: contains the html fragments used by the sub-application.
- `rpc.py`: contains the RPC caller and RPC callee that enables nodes to get information from other nodes from across the network.
- `utils.py`: contains helper functions.
- `connector.py`: contains the `Connector` class that is used to enable nodes to signal each other across the network. The connector class is not meant to be inherited. 

Also notice that the node class inside each subpackage inherits from the node class inside the parent package (e.g., FilesystemStoreNode  inherits from BaseResourceNode which inherits from BaseNode). 

`anacostia_pipeline/pipelines` - this is where all the pipelines live (e.g., `RootPipeline` and `LeafPipeline`)

`anacostia_pipeline/services` - this is where all the services live (e.g., `RootService` and `LeafService`)
