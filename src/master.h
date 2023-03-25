#pragma once

#include <chrono>
#include "mapreduce_spec.h"
#include "file_shard.h"
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc/grpc.h>
#include <grpc/support/log.h>
#include <memory>
#include <queue>
#include <thread>
#include <zconf.h>

#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

#define TIME_OUT 10
#define CONNECTION_TIME_OUT 1000

enum WorkType {
    MAP,
    REDUCE,
};

enum RequestStatus {
    NOT_STARTED = 0,
    PROCESSING = 1,
    FINISHED = 2
};

struct WorkerData {
    enum WorkerStatus {
        Busy = 1,
        Idle = 2,
        Down = 3,
    };

    WorkerData(std::string worker_addr, int worker_id):
        worker_addr_(std::move(worker_addr)),
        status_(Idle),
        worker_id(worker_id)
    {}

    std::string worker_addr_;
    WorkerStatus status_;
    int worker_id;
};


class WorkerClient {
public:
    WorkerClient(WorkerData worker_data): worker_metadata(worker_data) {
        // std::cout << "master connecting to port: " << worker_data.worker_addr_ << std::endl;
        channel = grpc::CreateChannel(worker_data.worker_addr_, grpc::InsecureChannelCredentials());
        stub_ = masterworker::WorkerService::NewStub(channel);
    }

    void set_worker_status(WorkerData::WorkerStatus status) {
        worker_metadata.status_ = status;
    }

    WorkerData::WorkerStatus get_worker_status() {
        return worker_metadata.status_;
    }

    int get_worker_id() {
        return worker_metadata.worker_id;
    }

    masterworker::MapReply get_map_reply() {
        return std::static_pointer_cast<MapAsyncContextManager>(context_manager)->reply;
    }

    void SendMapRequestToWorker(const masterworker::MapRequest &request) {
        if (worker_metadata.status_ != WorkerData::Idle) {
            throw 1;
        }
        worker_metadata.status_ = WorkerData::Busy;
        context_manager = std::make_shared<MapAsyncContextManager>();
        std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() + std::chrono::seconds(TIME_OUT);
        context_manager->context.set_deadline(deadline);
        std::shared_ptr<MapAsyncContextManager> map_context = std::static_pointer_cast<MapAsyncContextManager>(context_manager);
        map_context->rpc = stub_->PrepareAsyncRegisterMapService(&context_manager->context, request, &context_manager->cq);
        map_context->rpc->StartCall();
        map_context->rpc->Finish(&map_context->reply, &context_manager->status, (void*) 1);
    }

    void SendReduceRequestToWorker(const masterworker::ReduceRequest &request) {
        if (worker_metadata.status_ != WorkerData::Idle) {
            throw 1;
        }
        worker_metadata.status_ = WorkerData::Busy;
        context_manager = std::make_shared<ReduceAsyncContextManager>();
        std::chrono::system_clock::time_point deadline =
                std::chrono::system_clock::now() + std::chrono::seconds(TIME_OUT);
        context_manager->context.set_deadline(deadline);
        std::shared_ptr<ReduceAsyncContextManager> reduce_context = std::static_pointer_cast<ReduceAsyncContextManager>(context_manager);
        reduce_context->rpc = stub_->PrepareAsyncRegisterReduceService(&context_manager->context, request, &context_manager->cq);
        reduce_context->rpc->StartCall();
        reduce_context->rpc->Finish(&reduce_context->reply, &context_manager->status, (void*) 1);
    }

    bool check_status() {
        void* got_tag;
        bool ok = false;
        std::chrono::system_clock::time_point delay = std::chrono::system_clock::now() + std::chrono::seconds(TIME_OUT);
        GPR_ASSERT(context_manager->cq.Next(&got_tag, &ok));
        GPR_ASSERT(got_tag == (void*)1);
        GPR_ASSERT(ok);
        if (context_manager->status.ok())
        {
            worker_metadata.status_ = WorkerData::Idle;
            return true;
        } else {
            worker_metadata.status_ = WorkerData::Down;
            return false;
        }

    }

public:
    std::shared_ptr<grpc::Channel> channel;
private:
    struct AsyncContextManager {
        grpc::ClientContext context;
        grpc::CompletionQueue cq;
        grpc::Status status;
    };

    struct MapAsyncContextManager: AsyncContextManager {
        masterworker::MapReply reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::MapReply>> rpc;
    };

    struct ReduceAsyncContextManager: AsyncContextManager {
        masterworker::ReduceReply reply;
        std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::ReduceReply>> rpc;
    };
private:
    std::unique_ptr<masterworker::WorkerService::Stub> stub_;

    std::shared_ptr<AsyncContextManager> context_manager;
    WorkerData worker_metadata;
};


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

		bool task_all_finished(const std::vector<RequestStatus>& status) {
		    for (auto stats : status) {
		        if (stats != FINISHED) return false;
		    }
            return true;
		}

    private:
	    void assign_available_worker_to_jobs(WorkType work_type);
		void check_reply_and_update_status(std::vector<RequestStatus>& request_status,WorkType work_type);
        void retest_connection(WorkerClient* worker);

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::vector<masterworker::MapRequest> map_reqs;
		std::vector<RequestStatus> map_req_status;
		std::vector<masterworker::ReduceRequest> reduce_reqs;
        std::vector<RequestStatus> reduce_req_status;
        std::queue<WorkerClient*> active_clients;
        std::vector<WorkerClient> worker_clients;
        std::unordered_map<int, int> worker_job_tracker;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
    for (int worker_id = 0; worker_id < mr_spec.num_workers; worker_id++) {
        WorkerData cur_data = WorkerData(mr_spec.worker_ipaddr[worker_id], worker_id);
        worker_clients.emplace_back(WorkerClient(cur_data));
    }
    for (int shard_id = 0; shard_id < file_shards.size(); shard_id++) {
        masterworker::MapRequest new_request;
        new_request.set_user_id(mr_spec.user_id);
        new_request.set_shard_id(shard_id);
        new_request.set_n_output(mr_spec.num_outputs);
        new_request.set_output_dir(mr_spec.output_dir);
        for (const ShardInfo& shard_info : file_shards[shard_id].shards) {
            masterworker::ShardInfo* new_shard_info = new_request.add_shards();
            new_shard_info->set_file_addr(shard_info.file_name);
            new_shard_info->set_start_off(shard_info.start_off);
            new_shard_info->set_end_off(shard_info.end_off);
        }
        map_reqs.emplace_back(new_request);
        map_req_status.emplace_back(NOT_STARTED);
    }
    for (int reduce_id = 0; reduce_id < mr_spec.num_outputs; reduce_id++) {
        masterworker::ReduceRequest new_reduce_request;
        new_reduce_request.set_output_dir(mr_spec.output_dir);
        new_reduce_request.set_user_id(mr_spec.user_id);
        new_reduce_request.set_reducer_id(reduce_id);
        reduce_reqs.emplace_back(new_reduce_request);
        reduce_req_status.emplace_back(NOT_STARTED);
    }
}

void Master::assign_available_worker_to_jobs(WorkType work_type) {
    for (auto& worker_client : worker_clients) {
        if (worker_client.get_worker_status() != WorkerData::Idle) continue;
        auto &request_status = work_type == MAP ? map_req_status : reduce_req_status;
        for (int request_id = 0; request_id < request_status.size(); request_id++) {
            if (request_status[request_id] == NOT_STARTED) {
                request_status[request_id] = PROCESSING;
                worker_job_tracker[worker_client.get_worker_id()] = request_id;
                if (work_type == MAP) worker_client.SendMapRequestToWorker(map_reqs[request_id]);
                else worker_client.SendReduceRequestToWorker(reduce_reqs[request_id]);
                active_clients.push(&worker_client);
                break;
            }
        }
    }
}

void Master::check_reply_and_update_status(std::vector<RequestStatus>& request_status, WorkType work_type) {
    while (!active_clients.empty()) {
        WorkerClient* current_worker = active_clients.front();
        active_clients.pop();
        bool done = current_worker->check_status();
        int worker_id = current_worker->get_worker_id();
        if (done) {
            request_status[worker_job_tracker[worker_id]] = FINISHED;
            if (work_type == REDUCE) return;
            for (int reducer_id = 0; reducer_id < reduce_reqs.size(); reducer_id++) {
                reduce_reqs[reducer_id].add_intermediate_file_address(current_worker->get_map_reply().intermediate_file_location(reducer_id));
            }
        } else {
            request_status[worker_job_tracker[worker_id]] = NOT_STARTED;
            new std::thread(&Master::retest_connection, this, current_worker);
        }
    }
}

void Master::retest_connection(WorkerClient* worker) {
    while (!worker->channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(CONNECTION_TIME_OUT))) {}
    worker->set_worker_status(WorkerData::Idle);
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    while (!task_all_finished(map_req_status)) {
        assign_available_worker_to_jobs(MAP);
        check_reply_and_update_status(map_req_status, MAP);
    }
    while (!task_all_finished(reduce_req_status)) {
        assign_available_worker_to_jobs(REDUCE);
        check_reply_and_update_status(reduce_req_status, REDUCE);
    }
	return true;
}