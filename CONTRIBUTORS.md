# Welcome to the Anacostia Pipeline!


###Requirements: 
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

###How to install the pipeline:

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

###Packaging and publishing to PyPI:
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

###Anacostia Pipeline Modules:
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
