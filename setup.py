from setuptools import setup, find_packages
import os
import fnmatch
import pathlib
import shutil


def package_files(directory):
    """
    Recursively collects file paths within a directory relative to the anacostia_pipeline directory.
    """
    anacostia_pipeline_dir = os.path.abspath("anacostia_pipeline")
    paths = []
    exclude = ["*__pycache__*", "*.pyc", "*.pyo", "*.DS_Store"]
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            pathname = os.path.relpath(os.path.join(root, filename), anacostia_pipeline_dir)
            if not any([fnmatch.fnmatch(pathname, pattern) for pattern in exclude]):
                paths.append(pathname)
    return paths

static_files = package_files("anacostia_pipeline/static")

# removing dist/ and anacostia_pipeline.egg-info/ directories
shutil.rmtree("dist", ignore_errors=True)
shutil.rmtree("anacostia_pipeline.egg-info", ignore_errors=True)


setup(
    name="anacostia_pipeline",
    version="0.7.11",
    description="A framework for building MLOps pipelines",
    author="Minh-Quan Do",
    author_email="mdo9@gmu.edu",
    long_description=pathlib.Path("README.md").read_text(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    package_data={
        'anacostia_pipeline': [*static_files]
    },
    include_package_data=True,
    exclude_package_data={
        '': ['__pycache__', '*.pyc', '*.pyo']
    },
    install_requires=[
        "sqlalchemy>=1.4.0",
        "networkx==3.1",
        "markdown",
        "jinja2>=3.0.0",
        "pydantic",
        "fastapi", 
        "uvicorn[standard]",
        "httpx" 
    ],
    extras_require={
        "aws": ["boto3"]
    }
)