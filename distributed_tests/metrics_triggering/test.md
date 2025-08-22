### Test Objective:
Show pipeline can trigger properly upon the metrics in the metadata store surpassing a threshold.

### Pipeline Configuration:
Pipeline in `test.py`:
- Running on https://127.0.0.1:8000
- Nodes:
    - `metadata_store`
        - Type: `SQLiteMetadataStoreNode`
        - Running on https://127.0.0.1:8000/metadata_store
        - Purpose: to provide a central metadata store and to trigger the pipeline based on the `percent_accuracy` metric surpassing a threshold (0.4).
        - Database location: `./root-artifacts/input_artifacts/metadata_store/metadata.db`
    - `data_store`
        - Type: `FilesystemStoreNode`
        - Running on https://127.0.0.1:8000/data_store
        - Purpose: to be used as a placeholder.
        - Successors: `logging_node`
    - `logging_node`
        - Type: `BaseActionNode`
        - Running on https://127.0.0.1:8000/logging_node
        - Purpose: only here to provide a placeholder.

`log_metrics.py`:
- `edge_deployment_client` (SQLMetadataStoreClient) that logs metrics to the metadata store node.

### Test Setup:
Pipeline in `test.py` starts running. `log_metrics.py` will then start running. 

### Pipeline Trigger:
Pipeline will pull all metrics being logged prior to a run. 
If the highest metric recorded for that run is greater than 0.4, the pipeline will trigger.

### Instructions to run test:
Run `run_test.sh` file to automatically run tests.
To run tests manually:
1. Open up two terminals
2. Run `python setup.py` in terminal 1
3. Run `python test.py` in terminal 1
5. Run `python log_metrics.py` in terminal 2
6. Open up a browser and navigate to https://127.0.0.1:8000 to see the GUI
