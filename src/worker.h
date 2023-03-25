#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <fstream>
#include <sstream>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker : public masterworker::WorkerService::Service {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

        grpc::Status RegisterMapService(::grpc::ServerContext* context, const ::masterworker::MapRequest* request, ::masterworker::MapReply* response) override;
        grpc::Status RegisterReduceService(::grpc::ServerContext* context, const ::masterworker::ReduceRequest* request, ::masterworker::ReduceReply* response) override;

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string ip_addr_port_;
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
    ip_addr_port_ = std::move(ip_addr_port);
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	grpc::ServerBuilder builder;
	builder.AddListeningPort(ip_addr_port_, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    server->Wait();
	return true;
}


grpc::Status Worker::RegisterMapService(::grpc::ServerContext* context, const ::masterworker::MapRequest* request, ::masterworker::MapReply* response) {
    std::string output_dir = request->output_dir();
    std::string user_id = request->user_id();
    uint32_t num_output = request->n_output();
    uint32_t shard_id = request->shard_id();
    std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(user_id);
    mapper->impl_->set_metadata(output_dir, num_output, shard_id, user_id);
    for (masterworker::ShardInfo shard_info : request->shards()) {
        std::string file_path = shard_info.file_addr();
        int start_off = shard_info.start_off();
        int end_off = shard_info.end_off();
        std::ifstream file(file_path, std::ifstream::binary);
        file.seekg(start_off, std::istream::ios_base::beg);
        std::string content;
        while (file.tellg() < end_off && getline(file, content)) {
            mapper->map(content);
        }
        file.close();
    }
    mapper->impl_->close_files();
    response->set_status(true);
    for (int i = 0; i < mapper->impl_->output_files.size(); i++)
        response->add_intermediate_file_location(mapper->impl_->output_files[i]);
    return grpc::Status();
}

grpc::Status Worker::RegisterReduceService(::grpc::ServerContext* context, const ::masterworker::ReduceRequest* request, ::masterworker::ReduceReply* response) {
    std::string user_id = request->user_id();
    std::string output_dir = request->output_dir();
    std::vector<std::string> intermediate_input_files;
    int reducer_id = request->reducer_id();
    for (int file_id = 0; file_id < request->intermediate_file_address_size(); file_id++)
        intermediate_input_files.emplace_back(request->intermediate_file_address(file_id));
    std::shared_ptr<BaseReducer> reducer = get_reducer_from_task_factory(user_id);
    reducer->impl_->set_metadata(output_dir, reducer_id, user_id);
    std::map<std::string, std::vector<std::string>> combined_resource;
    std::string line;
    for (std::string file_path : intermediate_input_files) {
        std::ifstream file(file_path);
        while (getline(file, line)) {
            std::istringstream iss(line);
            std::string key, value;
            iss >> key;
            iss >> value;
            combined_resource[key].emplace_back(value);
        }
        file.close();
    }
    for (auto iter = combined_resource.begin(); iter != combined_resource.end(); iter++)
        reducer->reduce(iter->first, iter->second);
    reducer->impl_->close_file();
    return grpc::Status();
}