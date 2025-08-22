### Test Objective:
Show the simplest example possible of two pipeline signalling each other over the network.

### Pipeline Configuration:
`test_pipeline`:
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
        - Purpose: to detect incoming files dumped into the `./root-artifacts/input_artifacts/data_store` folder and to trigger the pipeline.
        - Successors: `logging_root`
    - `logging_node`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8001/logging_root
        - Purpose: only here to provide a placeholder.
        - Remote successors: `shakespeare_eval` 

`shakespeare_eval_pipeline`:
- Running on https://127.0.0.1:8001
- Nodes: 
    - `shakespeare_eval`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8001/shakespeare_eval
        - Purpose: only here to provide a placeholder.

### Test Setup:
Spin up `shakespeare_eval_pipeline` pipeline and then spin up `test_pipeline`.

### Pipeline Trigger:
Files will be created and dumped into the `./root-artifacts/input_artifacts/data_store` folder. `data_store` node will monitor the folder and trigger pipeline upon new files being dumped into the folder.

### Pipeline operation:
Upon being triggered, `logging_node` will trigger `shakespeare_eval` over the network.

### Instructions to run test:
Run `run_test.sh` file to automatically run tests.
To run tests manually:
1. Open up three terminals
2. Run `python setup.py` in terminal 1
3. Run `python leaf.py "127.0.0.1" 8001` in terminal 2
4. Run `python root.py "127.0.0.1" 8000 "127.0.0.1" 8001` in terminal 3
5. Go back to terminal 1 and run `python create_files.py`
6. Open up a browser and navigate to https://127.0.0.1:8000 to see the GUI