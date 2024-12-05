## Tips on building your own plugins for Anacostia

### Metadata Store
1. Inherit the ```BaseMetadataStoreNode``` class 
2. Override all base methods that raise ```NotImplementedError``` with some default code
3. Start by implementing setup() method
    - Tip: don’t call ```Pipeline.launch_nodes()```, just call ```Pipeline.setup_nodes()```
    - Desired result: verify you can acquire, configure to work with the pipeline, and that the pipeline can be spun up
4. Implement the ```create_resource_tracker()``` method
    - Tip: use FilesystemStoreNode in your pipeline and set `monitoring=False`
5. Implement the ```start_run()```
    - Tip: replace ```Pipeline.setup_nodes()``` with ```Pipeline.launch_nodes()```
    - Tip: when you are running your test, you will notice the script hangs, this is normal. Terminate the script check the metadata store to see if runs are being produced and `end_time` is NULL for all runs. 
7. Implement the ```end_run()```
    - At this point, check the metadata store to see if `start_time` and `end_time` is not NULL for all runs
8. Implement the `entry_exists()` and `create_entry()` methods
    - Tip: set `monitoring=True` in the FilesystemStoreNode
    - Tip: implement a test case that puts files into your folder that are being watched by the FilesystemStoreNode
    - Tip: override the `custom_trigger()` method of FilesystemStoreNode like so:
      ```python
      def custom_trigger(self) -> bool:
          return super().trigger()
      ```
    - At this point, you should see the metadata store recording the newly created files as new entries
9. Implement the `get_num_entries()` method
    - Tip: remove the `custom_trigger()` in the FilesystemStoreNode child class
10. Implement the ```add_run_id()``` and ```add_end_time()``` methods
    - Note: `add_run_id()` needs to set the `run_id` for any artifact entry whose `run_id` is `NULL` and update the state of the entry from `new` to `current`
    - Note: `add_end_time()` needs to set the `end_time` for any artifact entry whose `end_time` is `NULL` and update the state of the entry from `current` to `old`
  
### Resource Node
1. Figure out a way to monitor your resource for incoming changes
2. Inherit the ```BaseResourceNode``` class 
3. Override all base methods that raise ```NotImplementedError``` with some default code
4. Start by implementing setup() method
    - Tip: don’t call ```Pipeline.launch_nodes()```, just call ```Pipeline.setup_nodes()```
    - Desired result: verify you can acquire, configure to work with the pipeline, and that the pipeline can be spun up
5. Implement the ```start_monitoring()``` method
    - Note: you must create a new thread and run your monitoring code there
    - Note: you must implement your triggering logic in the thread that monitors your code 
6. Implement the ```stop_monitoring()``` method
7. Implement the ```record_new()``` method
8. Implement the ```record_current()``` method

Note: See filesystem_store.py for an example of how to create your own resource node.

### Action Node
1. Inherit the ```BaseActionNode``` class 
2. Override the ```execute()``` method in ```BaseActionNode```
