# Welcome to the Anacostia Pipeline!


Requirements:
1. Python 3.11 or greater, if you need to update Python, here are the steps:

Updating Python on Mac
Follow these steps to update your Python installation to the latest version on Mac:
1. Check Current Python Version
Open a terminal and use the python3 --version command to check which Python version you have installed.
For example:

```
python3 --version 
Python 3.7.4
```

2. Install Homebrew
If you don't already have Homebrew installed, you can install it with:
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

```echo "alias python=/usr/local/bin/python3" >> ~/.zshrc
source ~/.zshrc
```
8. Confirm New Version
Check that the new version is now set as default:

```python --version```

How to install the pipeline:

1. Create the python virtual env
```
python3 -m venv venv
source venv/bin/activate
```
2. pip install requiresments.txt
   
```pip install -r requirements.txt```

4. Update the requirements.txt if neccessary

5. Once you have all requirements installed, let's try to run one of the tests:
Go into the tests folder and run test_phase1.py:
```python3 test_phase1.py```

6. Create a .env file inside the tests folder:
Insert these credentials and set them:
# .env
USERNAME=
PASSWORD=


Other notes:
If you already have a Python virtual environment set up, follow these steps:
1. Check Python version inside virtual environment:
```(venv) python --version```
For example:
```(venv) Python 3.8.2```
2. Deactivate the virtual environment:
```(venv) deactivate```
3. Check system Python version:
```python --version```

This may be older than your virtual environment version.
4. Check Python 3 version:
```python3 --version```
5. Create a new virtual environment with Python 3:
```python3 -m venv venv```
6. Activate the new virtual environment:
```source venv/bin/activate```
7. Confirm Python version is updated:
```(venv) python --version```

Understanding the repo:
1. Engine folder - has two important files 1. node.py - which contains all teh base classes for the action node and the resource nodes, other nodes inherit the base node logic. This is where all the synctionaization primitives are defined. 2. pipeline.py is our way of intereacting with the dag, that will have commands for staring, terminating, shutting down, pausing the node and pipelines, The ability to export the graph.json file is here as well. 
constants.py defines the different statuses we have in the pipeline for the nodes.
2. Resource Folder: Defines the basic anacostia resources that users have right off the bat. It has 3 files:
 2.1 data_store.py refines where the user would input their data files in order to be consumed by the pipeline. Ex: if user has folder of images, it would take that folder and start keeping track of the state for that folder via data_store.json
2.2 feature_store.py - default anocostia datastore, this file converts features into a numpy array and saves the numpy array as a .npy file and keeps track of the state.
2.3 model_registry - where models are going to be kept. Keeps state of models i.e current, old, and new(this is applicatable to anacostia resources)
3. Tests folder - where all the tests for pipeline and nodes reside
The most uptodate is tests_phase1.py which includes all teh tests for the phase 1 configuration which is the ETL pipeline.
tests_data_store.py includes all the tests for the data store
tests_feature_store includes all the tests for the feature store
tests_model_registry include all tests for model registry - this is currently not up to date
tests_utils. includes all functionaliteis for creating the tests
tests_node and test_basic includes all tests for nodes which might not be used anymore. We might get rid of these.

4. setup.py - the main folder that is used to turn ana into a python package that we can put on PyPI - python package index. Allows you to `pip install anacostia`


Future work:
1. Take the tests from phase1 and create the suite of tests for phase 2, which is a full retraining pipeline without model evaluation.
2. We will create a metadata store node using a database (Most likely Mongo)
3. We will upgrade phase 2 tests to include model evaluation (now you can have full retraining pipeline)


# Example Usage

```python
n1 = TrueNode(name='n1')
n2 = FalseNode(name='n2')
n3 = TrueNode(name='n3', listen_to=n1 | n2)
n4 = TrueNode(name="n4", listen_to=n3)

p1 = Pipeline(nodes=[n1,n2,n3,n4])
```
