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
3. Run test
```
cd tests
python3 graph_test.py
```

### Packaging and publishing to PyPI:
1. Build the package:
```
cd anacostia
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
    |   ├── app.py - contains BaseApp
    |   ├── fragments.py - contains html fragments for BaseApp
    |   ├── actions/
    |   |   └── node.py - contains BaseActionNode (inherits BaseNode)
    |   ├── metadata/
    |   |   ├── node.py - contains BaseMetadataStoreNode (inherits BaseNode)
    |   |   └── sqlite/
    |   |       ├── node.py - contains SqliteMetadataStoreNode (inherits BaseMetadataStoreNode)
    |   |       ├── app.py - contains SqliteMetadataStoreApp (inherits BaseApp)
    |   |       └── fragments.py - contains html fragments for SqliteMetadataStoreApp
    |   ├── resources/
    |   |   ├── node.py - contains BaseResourceNode (inherits BaseNode)
    |   |   └── filesystem/
    |   |       ├── node.py - contains FilesystemStoreNode (inherits BaseResourceNode)
    |   |       ├── app.py - contains FilesystemStoreApp (inherits BaseApp)
    |   |       └── fragments.py - contains html fragments for FilesystemStoreApp
    |   └── network/
    |       ├── receiver/
    |       |   ├── node.py - contains ReceiverNode (inherits BaseNode)
    |       |   └── app.py - contains ReceiverApp (inherits BaseApp)
    |       └── sender/
    |           ├── node.py - contains SenderNode (inherits BaseNode)
    |           └── app.py - contains SenderApp (inherits BaseApp)
    ├── pipelines/ 
    |   ├── leaf/
    |   |   ├── pipeline.py - contains LeafPipeline
    |   |   └── app.py - contains LeafPipelineApp
    |   └── root/
    |       ├── pipeline.py - contains RootPipeline
    |       └── app.py - contains RootPipelineApp
    ├── services/ 
    |   ├── leaf/
    |   |   └── app.py - contains LeafServiceApp
    |   └── root/
    |       └── app.py - contains RootServiceApp
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
- `node.py`: which contains the code for the actual node itself.
- `app.py`: which contains the code for the FastAPI sub-application that provides a custom GUI for visualizing information pertaining to the node.
- `fragments.py` - which contains the html fragments used by the sub-application.

Also notice that the node class inside each subpackage inherits from the node class inside the parent package (e.g., FilesystemStoreNode  inherits from BaseResourceNode which inherits from BaseNode). 

`anacostia_pipeline/pipelines` - this is where all the pipelines live (e.g., `RootPipeline` and `LeafPipeline`)

`anacostia_pipeline/services` - this is where all the services live (e.g., `RootService` and `LeafService`)
