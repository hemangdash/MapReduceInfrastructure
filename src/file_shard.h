#pragma once

#include <vector>
#include "mapreduce_spec.h"

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct ShardInfo {
    std::string file_name;
    int start_off;
    int end_off;
};

struct FileShard {
    std::vector<ShardInfo> shards;
};

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
    int shard_size = mr_spec.map_kilobytes * 1024;
    int start = 0, end = 0, rem = shard_size;
    FileShard cur_shard;
    for (auto &file_path : mr_spec.input_files) {
        std::ifstream infile(file_path, std::ifstream::binary);
        std::string line;
        while (getline(infile, line)) {
            end += (line.size() + 1);
            rem -= (line.size() + 1);
            if (rem <= 0) {
                cur_shard.shards.emplace_back(ShardInfo{file_path, start, end});
                fileShards.emplace_back(std::move(cur_shard));
                start = end;
                rem = shard_size;
            }
        }
        infile.close();
        if (rem != shard_size) cur_shard.shards.emplace_back(ShardInfo{file_path, start, end});
        start = 0;
        end = 0;
    }
    fileShards.emplace_back(std::move(cur_shard));
	return true;
}