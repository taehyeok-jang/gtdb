
#include <cassert>
#include <iostream>
#include <string>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"
#include <chrono>
#include <ctime> 

constexpr bool DEBUG = false;

uint64_t wake_timeout_ = 100;
uint64_t timeout_ = 2;

namespace buzzdb {

char* BufferFrame::get_data() { return data.data(); }

BufferFrame::BufferFrame()
    : page_id(INVALID_PAGE_ID),
      frame_id(INVALID_FRAME_ID),
      dirty(false),
      exclusive(false) {}

BufferFrame::BufferFrame(const BufferFrame& other)
    : page_id(other.page_id),
      frame_id(other.frame_id),
      data(other.data),
      dirty(other.dirty),
      exclusive(other.exclusive) {}

BufferFrame& BufferFrame::operator=(BufferFrame other) {
  std::swap(this->page_id, other.page_id);
  std::swap(this->frame_id, other.frame_id);
  std::swap(this->data, other.data);
  std::swap(this->dirty, other.dirty);
  std::swap(this->exclusive, other.exclusive);
  return *this;
}

BufferManager::BufferManager(size_t page_size, size_t page_count) {
  capacity_ = page_count;
  page_counter_ = 0;
  page_size_ = page_size;

  pool_.resize(capacity_);
  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    pool_[frame_id].reset(new BufferFrame());
    pool_[frame_id]->data.resize(page_size_);
    pool_[frame_id]->frame_id = frame_id;
  }
}

BufferManager::~BufferManager() {
	
  flush_all_pages();

  // TODO free all locks???
  std::lock_guard<std::mutex> lock(lock_table_mutex_);
  for (auto &entry : page_lock_table_) {
    // uint64_t page_id = entry.first;
    Lock &page_lock = entry.second;

    std::unique_lock<std::mutex> page_lock_guard(page_lock.mtx);

    page_lock.exclusive_lock_holder = 0;
    page_lock.shared_lock_holders.clear();
    
    page_lock.cv.notify_all();
  }
}


BufferFrame& BufferManager::fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive) {

  if (DEBUG) {
    std::cout << "fix_page() \n";
    std::cout << "\ttxn_id: " << txn_id << "\n";
    std::cout << "\tpage_id: " << page_id << "\n";
    std::cout << "\texclusive: " << exclusive << "\n";
  }

  if (exclusive) {
    acquire_exclusive_lock(txn_id, page_id);
  } else {
    acquire_shared_lock(txn_id, page_id);
  }

  // Existing logic to fetch and return the page
  if (page_id == INVALID_PAGE_ID) {
		std::cout << "INVALID FIX REQUEST \n";
		exit(-1);
	}

	/// Check if page is in buffer
	uint64_t page_frame_id = get_frame_id_of_page(page_id);
	if (page_frame_id != INVALID_FRAME_ID) {
		return *pool_[page_frame_id];
	}

//	std::cout << "Create page: " << page_id << "\n";

	// Create a new page
	uint64_t free_frame_id = page_counter_++;

	if(page_counter_ >= capacity_){
		std::cout << "Out of space \n";
		std::cout << page_counter_ << " " << capacity_ << "\n";
		exit(-1);
	}

	pool_[free_frame_id]->page_id = page_id;
	pool_[free_frame_id]->dirty = false;

	read_frame(free_frame_id);

	return *pool_[free_frame_id];
}

void BufferManager::unfix_page(uint64_t txn_id, BufferFrame& page, bool is_dirty) {
  if (DEBUG) {
    std::cout << "unfix_page() \n";
    std::cout << "\ttxn_id: " << txn_id << "\n";
    std::cout << "\tpage.page_id: " << page.page_id << "\n";
    std::cout << "\tis_dirty: " << is_dirty << "\n";
  }

  if (is_dirty) {
    page.mark_dirty();
  }

  release_lock(txn_id, page.page_id);
}

void  BufferManager::flush_all_pages(){
  if (DEBUG) {
    std::cout << "flush_all_pages() \n";
    std::cout << "capacity_: " << capacity_ << "\n";
  }
  

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->dirty == true) {
			write_frame(frame_id);
		}
	}
}

void  BufferManager::discard_all_pages(){
  if (DEBUG) {
    std::cout << "discard_all_pages() \n";
    std::cout << "capacity_: " << capacity_ << "\n";
  }

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		pool_[frame_id].reset(new BufferFrame());
		pool_[frame_id]->page_id = INVALID_PAGE_ID;
		pool_[frame_id]->dirty = false;
		pool_[frame_id]->data.resize(page_size_);
  }
}

uint64_t BufferManager::get_frame_id_of_page(uint64_t page_id){

	uint64_t page_frame_id = INVALID_FRAME_ID;

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->page_id == page_id) {
			page_frame_id = frame_id;
			break;
		}
	}

	return page_frame_id;
}

// Flush all dirty pages acquired by the transaction to disk
void BufferManager::flush_pages(uint64_t txn_id){

  if (DEBUG) {
    std::cout << "flush_pages() \n";
  }

  // For each page locked by txn_id, if it is dirty, flush it.
  // This is typically called on commit

  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    BufferFrame &frame = *pool_[frame_id];

    if (frame.page_id != INVALID_PAGE_ID) {
      std::lock_guard<std::mutex> lock(lock_table_mutex_);
      auto it = page_lock_table_.find(frame.page_id);
      if (it != page_lock_table_.end()) {
        Lock &page_lock = it->second;

        // If the transaction holds an exclusive lock or a shared lock, flush the page
        if (page_lock.exclusive_lock_holder == txn_id ||
            page_lock.shared_lock_holders.find(txn_id) != page_lock.shared_lock_holders.end()) {
          if (frame.dirty) {
            write_frame(frame_id);  // Write the dirty page to disk
            frame.dirty = false;   // Mark the page as clean
          }
        }
      }
    }
  }
}

// Discard all pages acquired by the transaction 
void BufferManager::discard_pages(uint64_t txn_id){

  if (DEBUG) {
    std::cout << "discard_pages() \n";
  }

  // For each page locked by txn_id, we could reset them from the buffer, etc.
  // In a more complete system, we’d also ensure the page is not pinned anymore.

  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    BufferFrame &frame = *pool_[frame_id];

    if (frame.page_id != INVALID_PAGE_ID) {
      std::lock_guard<std::mutex> lock(lock_table_mutex_);
      auto it = page_lock_table_.find(frame.page_id);
      if (it != page_lock_table_.end()) {
        Lock &page_lock = it->second;

        // If the transaction holds an exclusive lock or a shared lock, discard the page
        if (page_lock.exclusive_lock_holder == txn_id ||
            page_lock.shared_lock_holders.find(txn_id) != page_lock.shared_lock_holders.end()) {
          frame.page_id = INVALID_PAGE_ID;
          frame.dirty = false;
          frame.data.clear();   
          frame.data.resize(page_size_);
        }
      }
    }
  }
}

void BufferManager::transaction_complete(uint64_t txn_id){

    if (DEBUG) {
      std::cout << "transaction_complete() \n";
    }
    // 1) flush all dirty pages
    flush_pages(txn_id); 

    // 2) release all locks
    std::lock_guard<std::mutex> lock(lock_table_mutex_);
    for (auto &entry : page_lock_table_) {
      // uint64_t page_id = entry.first;
      Lock &page_lock = entry.second;
  
      std::unique_lock<std::mutex> page_lock_guard(page_lock.mtx);
  
      if (page_lock.exclusive_lock_holder == txn_id) {
        page_lock.exclusive_lock_holder = 0;
      }
      page_lock.shared_lock_holders.erase(txn_id);
  
      // Notify other waiting transactions
      page_lock.cv.notify_all();
    }
}

void BufferManager::transaction_abort(uint64_t txn_id){

    if (DEBUG) {
      std::cout << "transaction_abort() \n";
    }

    // 1) discard all changes
    discard_pages(txn_id);
    
    // 2) release all locks
    std::lock_guard<std::mutex> lock(lock_table_mutex_);
    for (auto &entry : page_lock_table_) {
      // uint64_t page_id = entry.first;
      Lock &page_lock = entry.second;
  
      std::unique_lock<std::mutex> page_lock_guard(page_lock.mtx);
  
      if (page_lock.exclusive_lock_holder == txn_id) {
        page_lock.exclusive_lock_holder = 0;
      }
      page_lock.shared_lock_holders.erase(txn_id);
  
      // Notify other waiting transactions
      page_lock.cv.notify_all();
    }
}

void BufferManager::read_frame(uint64_t frame_id) {
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->read_block(start, page_size_, pool_[frame_id]->data.data());
}

void BufferManager::write_frame(uint64_t frame_id) {
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->write_block(pool_[frame_id]->data.data(), start, page_size_);
}

////// 2-Phase Locking

void BufferManager::acquire_shared_lock(uint64_t txn_id, uint64_t page_id) {
  std::unique_lock<std::mutex> lock(lock_table_mutex_);
  auto &page_lock = page_lock_table_[page_id];

  std::unique_lock<std::mutex> page_lock_guard(page_lock.mtx);
  lock.unlock();  // release lock_table_mutex_ early

  // allow the transaction to acquire a shared lock if it already holds an exclusive lock
  if (page_lock.exclusive_lock_holder == txn_id) {
    return;
  }
  // TODO 
  // page_lock.shared_lock_holders.insert(txn_id);

  // wait until no exclusive lock is held
  // ASIS
  // page_lock.cv.wait(page_lock_guard, [&]() {
  //   return page_lock.exclusive_lock_holder == 0;
  // });
  // TOBE
   // Wait until no exclusive lock is held, with a timeout for deadlock detection
  if (!page_lock.cv.wait_for(page_lock_guard, std::chrono::milliseconds(timeout_), [&]() {
    return page_lock.exclusive_lock_holder == 0;
  })) {
    // Timeout occurred, assume deadlock and abort the transaction
    throw transaction_abort_error();
  }

  // add the transaction to the shared lock holders
  page_lock.shared_lock_holders.insert(txn_id);
}

void BufferManager::acquire_exclusive_lock(uint64_t txn_id, uint64_t page_id) {
  std::unique_lock<std::mutex> lock(lock_table_mutex_);
  auto &page_lock = page_lock_table_[page_id];

  std::unique_lock<std::mutex> page_lock_guard(page_lock.mtx);
  lock.unlock();  // release lock_table_mutex_ early

  if (page_lock.exclusive_lock_holder == txn_id) {
    return;
  }

    // allow the transaction to acquire an exclusive lock if it already holds a shared lock
  if (page_lock.shared_lock_holders.size() == 1 &&
    page_lock.shared_lock_holders.count(txn_id) == 1) {
  
      page_lock.shared_lock_holders.erase(txn_id);
      page_lock.exclusive_lock_holder = txn_id;
      return;
  }

  // wait until no other locks are held
  // ASIS
  // page_lock.cv.wait(page_lock_guard, [&]() {
  //   return page_lock.exclusive_lock_holder == 0 &&
  //          page_lock.shared_lock_holders.empty();
  // });
  // TOBE
  if (!page_lock.cv.wait_for(page_lock_guard, std::chrono::milliseconds(timeout_), [&]() {
    return page_lock.exclusive_lock_holder == 0 &&
           page_lock.shared_lock_holders.empty();
  })) {
    // Timeout occurred, assume deadlock and abort the transaction
    throw transaction_abort_error();
  }

  // set the exclusive lock holder
  page_lock.exclusive_lock_holder = txn_id;
}

void BufferManager::release_lock(uint64_t txn_id, uint64_t page_id) {
  std::unique_lock<std::mutex> lock(lock_table_mutex_);
  auto &page_lock = page_lock_table_[page_id];

  std::unique_lock<std::mutex> page_lock_guard(page_lock.mtx);
  lock.unlock();  // release lock_table_mutex_ early

  // remove the transaction from the lock holders
  if (page_lock.exclusive_lock_holder == txn_id) {
    page_lock.exclusive_lock_holder = 0;
  } else {
    page_lock.shared_lock_holders.erase(txn_id);
  }

  // notify other waiting transactions
  page_lock.cv.notify_all();
}


}  // namespace buzzdb
