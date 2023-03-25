# cs6210Project4
MapReduce Infrastructure

## Project Instructions

[Project Description](description.md)

[Code walk through](structure.md)

### How to setup the project  
Same as project 3 instructions

### Introduction
This project was done solely by Hemang Dash.

In this project, we implement a simplified version of the Map Reduce infrastructure.

The files edited are:
- `masterworker.proto`
- `file_shard.h`
- `mapreduce_spec.h`
- `master.h`
- `mr_tasks.h`
- `worker.h`


### 1. `masterworker.proto`
In this file, we define the gRPC protocol between the master and the worker.
- The `WorkerService` has two functions to register the map service and the reduce service respectively.
- The `MapReply` contains the status and the intermediate file location.
- The `ReduceReply` contains the status.
- The `MapRequest` contains the shard ID, the number of outputs, the user ID, the output directory and the number of shards.
- The `ShardInfo` contains the file address, the start offset and the end offset.
- The `ReduceRequest` contains the user ID, the output directory, the intermediate file address and the reducer ID.


### 2. `file_shard.h`
To hold information about the file splits, we define two structures: `ShardInfo` which contains the file name, the start offset and the end offset and the `FileShard` which contains a vector of shards.

We create the file shards from the list of input files, map of kilobytes, etc. using map reduce spec we populated. To do so we loop through the map reduce spec's input files. For each line, we set the end offset and the remaining bytes. If the remaining bytes is less than or equal to 0, we add to the file shards and reset the remaining bytes to shard size.

If we reach the end of the file and there is some content remaining, we add it to the current shard. We then reset the start and end offsets as we have arrived at a new file. We finally emplace the last shard outside the for loop.


### 3. `mapreduce_spec.h`
We create the `MapReduceSpec` data structure for storing spec from the config file. It contains the number of workers (`num_workers`), the map of kilobytes (`map_kilobytes`), the output directory (`output_dir`), the user ID (`user_id`), the list of worker IP address ports (`worker_ipaddr`) and the list of input files (`input_files`).

We populate the `MapReduceSpec` data structure with the specification from the config file in the `read_mr_spec_from_config_file()` function. If the key of the spec exists, the data structure is updated with the appropriate value and we return true. If the key specified does not exist, we return false.

Finally, we validate the specification read from the config file in the `validate_mr_spec()` function. The number of workers must not be zero and must be compatible with the passed in worker IP address ports. The number of output files should be larger than zero. The shard size must be greater than zero. The user ID must be set. If the input file or the output directory do not exist, the spec is not valid.


### 4. `master.h`
We define an enum `WorkType`; the `WorkType` can either be `MAP` or `REDUCE`. We define an enum `RequestStatus`; the `RequestStatus` can either be `NOT_STARTED` or `PROCESSING` or `FINISHED`. We define a structure `WorkerData`. It contains an enum `WorkerStatus` (`Busy`, `Idle`, `Down`), the worker IP address, the status and the worker ID.

We define a class `WorkerClient`. The constructor connects the master to the port passed in the `worker_data` variable. To send a map request to the worker, we define a `SendMapRequestToWorker()` function. To send a reduce request to the worker, we define a `SendReduceRequestToWorker()` function. If the worker is not idle, we throw an error as we must send the request to an idle worker. To check the status of the worker, we use a `check_status()` function. If the result is finished, we set the status to be idle indicating that the worker is ready for another work.

In the `Master` class, we add a vector of `map_reqs`, a vector of `map_req_status`, a vector of `reduce_reqs`, a vector of `reduce_req_status`, a queue of `active_clients`, a vector of `worker_clients` and an unordered map for tracking the worker jobs (`worker_job_tracker`). We also declare three functions: `assign_available_worker_to_jobs()`, `check_reply_and_update_status()` and `retest_connection()`. A public function that is defined is `task_all_finished()` to see if all the statuses of all the tasks are `FINISHED` or not.

In the `Master` constructor, we populate he map requests and reduce requests. To run the `Master`, we loop through the `map_request_status_` until all the tasks are finished. We assign an available worker to the jobs and then check the reply and update the status. If the map is finished, we try to start the reduce tasks and do the same. In the `check_reply_and_update_status()` function, we find the current worker as the worker in the front of the `active_clients` queue. We see if the status of the current worker is done. If it is done, we set the request status to finished. If we are doing a map job, we populate the intermediate file address file. If it is not done, the worker is down and we must requeue the job. We spawn a new thread to try to reconnect to the worker client.


### 5. `mr_tasks.h`
In the `BaseMapperInternal` structure, we define a function `close_files()` to close all the files and a function to set the metadata (`set_metadata()`), which is used in `worker.h`. We also define some of the `MapReduceSpec`'s data members as the data members in this class: the output directory (`output_dir`), the user ID (`user_id`), the number of outputs (`num_output`), a vector of output file iterators (`output_file_iter`) and a vector of output files (`output_files`).

In the `BaseReducerInternal` structure, we define a function `close_files()` to close all the files and a function to set the metadata (`set_metadata()`), which is used in `worker.h`. We also define some data members: the reducer ID (`reducer_id`), the output file (`out_file`) and the user ID (`user_id`).


### 6. `worker.h`
In the `Worker` class, we define a `RegisterMapService()` function and a `RegisterReduceService()` function. We define an IP address port (on which the worker listens) data member for the class. This is set in the constructor.

To run, we use a `ServerBuilder` which adds the listening port, registers the service and the server is built and started. We wait for the server to finish.

In the `RegisterMapService()` function, we receive the map request which contains the user ID and the shard ID. First, we set the metadata of the mapper. Then, for each shard, we seek the file stream from the beginning and map the content.

Similarly, In the `RegisterReduceService()` function, we receive the reduce request which contains the user ID and the shard ID. First, we get the input files of the reducer. We then set the metadata of the reducer. Then, for each intermediate input file, we update the combined resource's key with the appropriate value.