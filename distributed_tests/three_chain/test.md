### Test Objective:
Show how three pipelines can chained together.

### Pipeline Configuration:
`root_pipline`:
- Server running on https://127.0.0.1:8000
- Nodes:
    - `metadata_store`
        - Type: `SQLiteMetadataStoreNode`
        - Running on https://127.0.0.1:8000/metadata_store
        - Purpose: to provide a central metadata store between the predecessor and successor pipeline.
        - Database location: `./root-artifacts/input_artifacts/metadata_store/metadata.db`
        - Remote successors: `data_store`
    - `data_store`
        - Type: `FilesystemStoreNode`
        - Running on https://127.0.0.1:8001/data_store
        - Storage directory: `./root-artifacts/input_artifacts/data_store`
        - Purpose: to detect incoming files dumped into the `./leaf-artifacts/input_artifacts/data_store` folder and to trigger the pipeline.
        - Successors: `logging_root`
    - `logging_root`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8001/logging_root
        - Purpose: only here to provide a placeholder.
        - Remote successors: `logging_leaf_1` 

`leaf1`:
- Running on https://127.0.0.1:8001
- Nodes: 
    - `logging_leaf_1`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8001/logging_leaf_1
        - Purpose: only here to provide a placeholder.
        - Remote successors: `logging_leaf_2` 

`leaf2`:
- Running on https://127.0.0.1:8002
- Nodes: 
    - `logging_leaf_2`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8001/logging_leaf_2
        - Purpose: only here to provide a placeholder.

### Test Setup:
Spin up `leaf1` pipeline, then `leaf2` pipeline, and then spin up `root_pipline`.

### Pipeline Trigger:
Files will be created and dumped into the `./root-artifacts/input_artifacts/data_store` folder. `data_store` node will monitor the folder and trigger pipeline upon new files being dumped into the folder.

### Pipeline operation:
Upon being triggered, `logging_root` will trigger `logging_leaf_1` which will then trigger `logging_leaf_2` over the network.

### Instructions to run test:
Run `run_test.sh` file to automatically run tests.
To run tests manually:
1. Open up four terminals
2. Run `python setup.py` in terminal 1
3. Run `python leaf2.py "127.0.0.1" 8002` in terminal 2
4. Run `python leaf1.py "127.0.0.1" 8001 "127.0.0.1" 8002` in terminal 3
5. Run `python root.py "127.0.0.1" 8000 "127.0.0.1" 8001` in terminal 4
6. Go back to terminal 1 and run `python create_files.py`
7. Open up a browser and navigate to https://127.0.0.1:8000 to see the GUI