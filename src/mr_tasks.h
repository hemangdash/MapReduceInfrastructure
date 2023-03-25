#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <fstream>

/* CS6210_TASK Implement this data structure as per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void set_metadata(std::string output_dir, int num_ouput, int shard_id, std::string user_id) {
		    output_dir = output_dir;
		    num_output = num_ouput;
		    for (int i = 0; i < num_ouput; i++) {
		        std::string output_file_path = output_dir + "/" + user_id + "_intermediate_shard_" + std::to_string(shard_id) + "_" + std::to_string(i);
		        output_files.emplace_back(output_file_path);
		        output_file_iter.emplace_back(std::ofstream(output_files[i], std::ios::binary | std::ios::app));
		    }
		}

		void close_files() {
		    for (auto& output_iter : output_file_iter) output_iter.close();
		}

		std::string output_dir;
		std::string user_id;
		int num_output;
		std::vector<std::ofstream> output_file_iter;
		std::vector<std::string> output_files;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
    output_file_iter[std::hash<std::string>{}(key) % num_output] << key << " " << val << "\n";
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation */
		void set_metadata(std::string output_dir, int reducer_id, std::string user_id) {
		    std::string output_file_path = output_dir + "/" + user_id + "_result_" + std::to_string(reducer_id);
            this->reducer_id = reducer_id;
            out_file = std::ofstream(output_file_path, std::ios::binary | std::ios::app);
		}

		void close_file() {
			out_file.close();
		}

		int reducer_id;
		std::ofstream out_file;
		std::string user_id;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    out_file << key << " " << val << "\n";
}