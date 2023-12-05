from setuptools import setup, find_packages

setup(
    name="anacostia_pipeline",
    version="0.0.1",
    description="A framework for building MLOps pipelines",
    author="Minh-Quan Do",
    author_email="mdo9@gmu.edu",
    packages=find_packages(),
    install_requires=[
        "networkx==3.1",
        "watchdog==3.0.0",
        "numpy",
        "pydantic",
        "rich"
    ],
    extras_require={
        "web": ['fastapi', 'uvicorn[standard]']
    }
)