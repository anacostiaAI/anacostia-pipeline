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

static_files = package_files("anacostia_pipeline/dashboard/static")

# removing dist/ and anacostia_pipeline.egg-info/ directories
shutil.rmtree("dist", ignore_errors=True)
shutil.rmtree("anacostia_pipeline.egg-info", ignore_errors=True)


# note: the non-web version of the package is broken because it does not install the web dependencies
# consider making the web version of the package the default
# remove beautifulsoup4 from the web dependencies; it's only used to implement the UI for a plotting node
setup(
    name="anacostia_pipeline",
    version="0.2.2",
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
        "networkx==3.1",
        "pydantic",
        "rich",
        "sqlalchemy",
        "fastapi", 
        "uvicorn[standard]" 
    ],
)