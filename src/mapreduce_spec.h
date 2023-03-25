#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <exception>
#include <iostream>
#include <sys/stat.h>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
    int num_workers = 0;
    int map_kilobytes = 0;
    int num_outputs = 0;
    std::string output_dir = "";
    std::string user_id = "";
    std::vector<std::string> worker_ipaddr;
    std::vector<std::string> input_files;
};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
    std::ifstream config_file_reader(config_filename, std::ifstream::in);
    std::string line, key, value;
    while (getline(config_file_reader, line)) {
        std::istringstream is_line(line);
        if (std::getline(is_line, key, '=') && std::getline(is_line, value)) {
            if (key == "n_workers") mr_spec.num_workers = std::stoi(value);
            else if (key == "worker_ipaddr_ports") {
                std::string addr;
                std::istringstream stream(value);
                while (getline(stream, addr, ',')) mr_spec.worker_ipaddr.emplace_back(addr);
            } else if (key == "input_files") {
                std::string filesaddr;
                std::istringstream stream(value);
                while (getline(stream, filesaddr, ',')) mr_spec.input_files.emplace_back(filesaddr);
            }
            else if (key == "output_dir") mr_spec.output_dir = value;
            else if (key == "n_output_files") mr_spec.num_outputs = std::stoi(value);
            else if (key == "map_kilobytes") mr_spec.map_kilobytes = std::stoi(value);
            else if (key == "user_id") mr_spec.user_id = value;
            else return false;
        }
    }
    return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
    bool is_valid = true;
    if (mr_spec.num_workers == 0 || mr_spec.num_workers != mr_spec.worker_ipaddr.size()) is_valid = false;
    if (mr_spec.num_outputs <= 0) is_valid = false;
    if (mr_spec.map_kilobytes <= 0) is_valid = false;
    if (mr_spec.user_id.empty()) is_valid = false;
    for (auto& file_path : mr_spec.input_files) {
        std::ifstream file(file_path);
        if (file.fail()) is_valid = false;
    }
    struct stat buffer;
    if (stat(mr_spec.output_dir.c_str(), &buffer) != 0) is_valid = false;
	return is_valid;
}