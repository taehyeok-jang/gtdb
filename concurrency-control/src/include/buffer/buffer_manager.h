#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <set>

#include <condition_variable>

#include "common/macros.h"
//#include "common/recursive_shared_mutex.h"

namespace buzzdb {

class BufferFrame {
 private:
	friend class BufferManager;

	uint64_t page_id;
	uint64_t frame_id;
	std::vector<char> data;

	bool dirty;
	bool exclusive;
	std::thread::id exclusive_thread_id;

 public:
	/// Returns a pointer to this page's data.
	char *get_data();

	BufferFrame();

	BufferFrame(const BufferFrame &other);

	BufferFrame &operator=(BufferFrame other);

	void mark_dirty() {dirty = true;}
};

class buffer_full_error : public std::exception {
 public:
	const char *what() const noexcept override { return "buffer is full"; }
};

class transaction_abort_error : public std::exception {
 public:
	const char *what() const noexcept override { return "transaction aborted"; }
};

class BufferManager {
 public:
	/// Constructor.
	/// @param[in] page_size  Size in bytes that all pages will have.
	/// @param[in] page_count Maximum number of pages that should reside in
	//                        memory at the same time.
	BufferManager(size_t page_size, size_t page_count);

	/// Destructor. Writes all dirty pages to disk.
	~BufferManager();

	BufferFrame &fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive);


	void unfix_page(uint64_t txn_id, BufferFrame& page, bool is_dirty);

	/// Returns the segment id for a given page id which is contained in the 16
	/// most significant bits of the page id.
	static constexpr uint16_t get_segment_id(uint64_t page_id) {
		return page_id >> 48;
	}

	/// Returns the page id within its segment for a given page id. This
	/// corresponds to the 48 least significant bits of the page id.
	static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
		return page_id & ((1ull << 48) - 1);
	}

	/// Returns the overall page id associated with a segment id and
	/// a given segment page id.
	static uint64_t get_overall_page_id(uint16_t segment_id, uint64_t segment_page_id) {
		return (static_cast<uint64_t>(segment_id) << 48) | segment_page_id;
	}

	/// Print page id
	static std::string print_page_id(uint64_t page_id) {
		if (page_id == INVALID_NODE_ID) {
			return "INVALID";
		} else {
			auto segment_id = BufferManager::get_segment_id(page_id);
			auto segment_page_id = BufferManager::get_segment_page_id(page_id);
			return "( " + std::to_string(segment_id) + " " +
						 std::to_string(segment_page_id) + " )";
		}
	}

	size_t get_page_size() { return page_size_; }

	void flush_all_pages();
	void flush_page(uint64_t page_id);
	void discard_page(uint64_t page_id);
	void discard_all_pages();

    /// Returns the frame id of the frame containing the page if it is present in the buffer.
    /// Otherwise, returns INVALID_FRAME_ID
    uint64_t get_frame_id_of_page(uint64_t page_id);

	// Flush all dirty pages acquired by the transaction to disk
	void flush_pages(uint64_t txn_id);
	// Discard all pages acquired by the transaction 
	void discard_pages(uint64_t txn_id);
	// Free all the locks acquired by the transaction
	void transaction_complete(uint64_t txn_id);
	// Free all the locks acquired by the transaction
	void transaction_abort(uint64_t txn_id);
 private:
	uint64_t capacity_;
	size_t page_size_;
	std::vector<std::unique_ptr<BufferFrame>> pool_;
	uint64_t page_counter_ = 0;

	mutable std::mutex file_use_mutex_;
	
	void read_frame(uint64_t frame_id);

	void write_frame(uint64_t frame_id);

	////// 2-Phase Locking
	struct Lock {
		std::mutex mtx;
		std::condition_variable cv;
		std::set<uint64_t> shared_lock_holders;  // txn_ids holding shared locks
		uint64_t exclusive_lock_holder = 0;     // txn_id holding the exclusive lock
	  };

	std::unordered_map<uint64_t, Lock> page_lock_table_;  // mappings page_id to its lock
	std::mutex lock_table_mutex_;                    // protects access to the lock table


	// TODO do we need this, too? 
	// // For each txn_id, track which pages it currently has locked
	// std::unordered_map<uint64_t, std::set<uint64_t>> txn_locks_;

	// helper methods for locking
	void acquire_shared_lock(uint64_t txn_id, uint64_t page_id);
	void acquire_exclusive_lock(uint64_t txn_id, uint64_t page_id);
	void release_lock(uint64_t txn_id, uint64_t page_id);

};

}  // namespace buzzdb
