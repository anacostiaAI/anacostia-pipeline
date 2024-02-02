from setuptools import setup, find_packages
import os
import fnmatch


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
template_files = package_files("anacostia_pipeline/templates")


setup(
    name="anacostia_pipeline",
    version="0.0.1",
    description="A framework for building MLOps pipelines",
    author="Minh-Quan Do",
    author_email="mdo9@gmu.edu",
    packages=find_packages(),
    package_data={
        'anacostia_pipeline': [*static_files, *template_files]
    },
    include_package_data=True,
    exclude_package_data={
        '': ['__pycache__', '*.pyc', '*.pyo']
    },
    install_requires=[
        "networkx==3.1",
        "pydantic",
        "rich" 
    ],
    extras_require={
        "web": ['fastapi', 'uvicorn[standard]', 'Jinja2', 'requests', "beautifulsoup4"]
    }
)