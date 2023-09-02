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
5. Install Latest Python
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

3. Update the requirements.txt if neccessary



# Example Usage

```python
n1 = TrueNode(name='n1')
n2 = FalseNode(name='n2')
n3 = TrueNode(name='n3', listen_to=n1 | n2)
n4 = TrueNode(name="n4", listen_to=n3)

p1 = Pipeline(nodes=[n1,n2,n3,n4])
```