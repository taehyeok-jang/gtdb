#include "external_sort/external_sort.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <thread>
#include <vector>

#include "storage/file.h"

#define UNUSED(p) ((void)(p))

namespace buzzdb {

struct MinHeapEntry {
  uint64_t value;
  size_t chunk_id;
  size_t index; // index in the in-memory buffer of that chunk

  bool operator>(const MinHeapEntry& other) const {
    return value > other.value;
  }
};

/**
 * @param file
 */
void _print(File& file, size_t num_values) {
            
    static const size_t CHUNK_SIZE = 512;
    std::vector<uint64_t> buffer(CHUNK_SIZE);

    size_t offset = 0;

    while (offset < num_values) {
        size_t to_read = std::min(CHUNK_SIZE, num_values - offset);

        file.read_block(
            offset * sizeof(uint64_t),             // byte offset 
            to_read * sizeof(uint64_t),            // bytes to read
            reinterpret_cast<char*>(buffer.data()) // destination
        );

        for (size_t i = 0; i < to_read; i++) {
            std::cout << buffer[i] << " ";
        }

        offset += to_read;
    }

    std::cout << std::endl;
    return;
}


void external_sort(File &input, size_t num_values, File &output, size_t mem_size) {

    // // XXX debug
    // std::cout << "[external_sort] arguments:\n";
    // std::cout << "  - input file size (bytes) : " << input.size() << "\n";
    // std::cout << "  - num_values              : " << num_values << "\n";
    // std::cout << "  - output file size (bytes): " << output.size() << "\n";
    // std::cout << "  - mem_size (bytes)        : " << mem_size << "\n";

    // // XXX debug
    // std::cout << "input: " << std::endl;
    // _print(input, num_values);
    

    // --------------------------------------------------------------------------
    // 1. SORT:
    //    sort chunks of data that fit into memory. 
    // 
    //    1) read chunk_size of uint_64 integers from the input file.
    //    2) sort the data in main memory. 
    //    3) write the sorted data to a file.
    //    4) repeat 1-3 until all data is sorted and written to files.
    // --------------------------------------------------------------------------

    // e.g. if mem_size = 8 * 1024 * 1024 (8 MB), then chunk_size = 1M 64-bit integers.
    const size_t chunk_size = mem_size / sizeof(uint64_t);
    if (chunk_size == 0) {
        return;
    }
    // num of chunks
    const size_t M = (num_values + chunk_size - 1) / chunk_size;

    std::vector<std::unique_ptr<File>> run_files;
    run_files.reserve(M);

    {
    size_t values_remaining = num_values;
    size_t offset = 0;  // in terms of 64-bit integers

    for (size_t i = 0; i < M; i++) {
        
        // // XXX debug
        // std::cout << "values_remaining...: " << values_remaining << "\n";

        size_t to_read = std::min(values_remaining, chunk_size);

        std::vector<uint64_t> buffer(to_read);
        input.read_block(
            offset * sizeof(uint64_t),
            to_read * sizeof(uint64_t),
            reinterpret_cast<char*>(buffer.data())
            );

        // in-memory sort
        std::sort(buffer.begin(), buffer.end());

        // a temporary file for this sorted chunk
        auto temp_file = File::make_temporary_file();
        temp_file->resize(to_read * sizeof(uint64_t));

        temp_file->write_block(
            reinterpret_cast<const char*>(buffer.data()),
            0, // offset in bytes
            to_read * sizeof(uint64_t));

        run_files.push_back(std::move(temp_file));

        values_remaining -= to_read;
        offset += to_read;
        }
    }

    // If we only have 1 chunk after the split, simply copy from that chunk to 'output'.
    if (M == 1) {
        output.resize(num_values * sizeof(uint64_t));
        if (num_values > 0) {
            auto chunk_data = run_files[0]->read_block(
                0, num_values * sizeof(uint64_t)
                );
            output.write_block(chunk_data.get(), 0, num_values * sizeof(uint64_t));
        }
        // // XXX debug
        // std::cout << "output (M==1): " << std::endl;
        // _print(output, num_values);

        return;
    }
    
    // --------------------------------------------------------------------------
    // 2. K-WAY MERGE PHASE:
    //    Merge M sorted runs using a min-heap, reading/writing in buffered chunks
    // 
    //    1) read the first buffer_size of each sorted chunk into input buffers in main memory
    //    2) perform a M-way merge and write the result to the output buffer. 
    //          whenever the output buffer fills, write it to the output file and empty the output buffer.
    //          whenever an input buffer is empty, refill it with the next buffer_size of data from the corresponding chunk.
    // --------------------------------------------------------------------------

    // partition mem_size for (M input buffers + 1 output buffer)
    // each of size 'buffer_size' (in 64-bit integers).
    // So, total memory usage (M+1) * buffer_size * sizeof(uint64_t) <= mem_size
    const size_t total_capacity_in_uint64 = mem_size / sizeof(uint64_t);
    const size_t buffer_size =
        (M + 1 <= total_capacity_in_uint64)
            ? (total_capacity_in_uint64 / (M + 1)) : 1; // fallback if M+1 > total capacity

    // Create input buffers for each run
    std::vector<std::vector<uint64_t>> input_buffers(M);
    std::vector<size_t> buffer_counts(M, 0); // num of elements that are currently loaded in each buffer
    std::vector<size_t> chunk_offsets(M, 0); // current offset in the chunk file (in 64 bytes)

    // Initialize each run by reading up to 'buffer_size' elements
    for (size_t i = 0; i < M; i++) {
        input_buffers[i].resize(buffer_size);
        size_t run_file_size = run_files[i]->size() / sizeof(uint64_t);

        size_t to_read = std::min(buffer_size, run_file_size);
        if (to_read > 0) {
            run_files[i]->read_block(
                0, // offset in bytes
                to_read * sizeof(uint64_t),
                reinterpret_cast<char*>(input_buffers[i].data())
                );
            buffer_counts[i] = to_read;
            chunk_offsets[i] = to_read;
        } else {
            buffer_counts[i] = 0;
            chunk_offsets[i] = 0;
        }
    }

    // Initialize a min-heap over (value, chunk_id, index_in_buffer).
    std::priority_queue<MinHeapEntry, std::vector<MinHeapEntry>, std::greater<MinHeapEntry>> min_heap;
    for (size_t i = 0; i < M; i++) {
        if (buffer_counts[i] > 0) {
            MinHeapEntry entry;
            entry.value = input_buffers[i][0];
            entry.chunk_id = i;
            entry.index = 0; // the first element
            min_heap.push(entry);
        }
    }

    // prepare the output file and its output buffer
    output.resize(num_values * sizeof(uint64_t));
    std::vector<uint64_t> output_buffer(buffer_size);
    size_t output_buffer_index = 0;
    size_t output_file_offset = 0;

    auto flush_output_buffer = [&]() {
        if (output_buffer_index > 0) {
            size_t bytes_to_write = output_buffer_index * sizeof(uint64_t);
            output.write_block(
                reinterpret_cast<const char*>(output_buffer.data()),
                output_file_offset * sizeof(uint64_t),
                bytes_to_write);
            output_file_offset += output_buffer_index;
            output_buffer_index = 0;
        }
    };

    while (!min_heap.empty()) {
        MinHeapEntry top = min_heap.top();
        min_heap.pop();

        output_buffer[output_buffer_index++] = top.value;
        if (output_buffer_index == buffer_size) {
            flush_output_buffer();
        }

        size_t c = top.chunk_id;
        size_t i = top.index + 1;
        if (i < buffer_counts[c]) {
            MinHeapEntry new_entry;
            new_entry.value = input_buffers[c][i];
            new_entry.chunk_id = c;
            new_entry.index = i;
            min_heap.push(new_entry);
        } else {
            // We've used up the buffer for chunk c; let's try to refill from disk
            size_t run_file_size = run_files[c]->size() / sizeof(uint64_t);
            // If chunk_offsets[c] < run_file_size, we can read more
            if (chunk_offsets[c] < run_file_size) {
                size_t to_read = std::min(buffer_size, run_file_size - chunk_offsets[c]);
                run_files[c]->read_block(
                    chunk_offsets[c] * sizeof(uint64_t),
                    to_read * sizeof(uint64_t),
                    reinterpret_cast<char*>(input_buffers[c].data())
                    );

                buffer_counts[c] = to_read;
                chunk_offsets[c] += to_read;

                if (to_read > 0) {
                    MinHeapEntry new_entry;
                    new_entry.value = input_buffers[c][0];
                    new_entry.chunk_id = c;
                    new_entry.index = 0;
                    min_heap.push(new_entry);
                }
            } else {
                buffer_counts[c] = 0;
            }
        }
    }

    flush_output_buffer();

    // // XXX debug
    // std::cout << "output: " << std::endl;
    // _print(output, num_values);

    return;
}

}  // namespace buzzdb
