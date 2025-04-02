
#include "log/log_manager.h"

#include <string.h>

#include <cassert>
#include <cstddef>
#include <iostream>
#include <cstring>
#include <set>
#include <unordered_set>

#include <iomanip>
#include <sstream>

#include "common/macros.h"
#include "storage/test_file.h"

namespace buzzdb {
////////////////////////////////////////////////////////////////////////////////

inline const char* to_string(LogManager::LogRecordType t) {
    switch (t) {
        case LogManager::LogRecordType::INVALID_RECORD_TYPE: return "INVALID_RECORD_TYPE";
        case LogManager::LogRecordType::ABORT_RECORD:        return "ABORT_RECORD";
        case LogManager::LogRecordType::COMMIT_RECORD:       return "COMMIT_RECORD";
        case LogManager::LogRecordType::UPDATE_RECORD:       return "UPDATE_RECORD";
        case LogManager::LogRecordType::BEGIN_RECORD:        return "BEGIN_RECORD";
        case LogManager::LogRecordType::CHECKPOINT_RECORD:   return "CHECKPOINT_RECORD";
    }
    // If somehow none of the cases matched (shouldn't happen),
    // fallback to an unknown string.
    return "UNKNOWN_RECORD_TYPE";
}

std::string to_hex_string(const std::vector<std::byte>& bytes) {
    std::ostringstream oss;
    oss << std::hex << std::setw(2) << std::setfill('0');
    for (auto b : bytes) {
        // 'static_cast<int>(b)' is needed to convert std::byte to an integral type
        oss << static_cast<int>(b) << " ";
    }
    return oss.str();
}

////////////////////////////////////////////////////////////////////////////////
/**
 * Functionality of the buffer manager that might be handy

 Flush all the dirty pages to the disk
        buffer_manager.flush_all_pages():

 Write @data of @length at an @offset the buffer page @page_id
        BufferFrame& frame = buffer_manager.fix_page(page_id, true);
        memcpy(&frame.get_data()[offset], data, length);
        buffer_manager.unfix_page(frame, true);

 * Read and Write from/to the log_file
   log_file_->read_block(offset, size, data);

   Usage:
   uint64_t txn_id;
   log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char *>(&txn_id));
   log_file_->write_block(reinterpret_cast<char *> (&txn_id), offset, sizeof(uint64_t));
 */

LogManager::LogManager(File* log_file) {
    log_file_ = log_file;
    log_record_type_to_count[LogRecordType::ABORT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::COMMIT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::UPDATE_RECORD] = 0;
    log_record_type_to_count[LogRecordType::BEGIN_RECORD] = 0;
    log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD] = 0;
}

LogManager::~LogManager() {}

void LogManager::reset(File* log_file) {
    log_file_ = log_file;
    current_offset_ = 0;
    txn_id_to_first_log_record.clear();
    log_record_type_to_count.clear();
}

/// Get log records
uint64_t LogManager::get_total_log_records() { 
    uint64_t total = 0;
    for (const auto& kv : log_record_type_to_count) {
        total += kv.second;
    }
    return total;
}

uint64_t LogManager::get_total_log_records_of_type(UNUSED_ATTRIBUTE LogRecordType type) {
    return log_record_type_to_count[type];
}

void LogManager::write_log_record(const LogRecordHeader& record) {
    
    // std::cout << "write_log_record: " << current_offset_ << std::endl;

    log_file_->resize(current_offset_ + record.rec_size);
    // 
    auto record_type = record.type;
    log_file_->write_block(
        reinterpret_cast<const char*>(&record_type),
        current_offset_,
        sizeof(record_type)
    );
    current_offset_ += sizeof(record_type);

    uint64_t sz = record.rec_size; 
    log_file_->write_block(
        reinterpret_cast<const char*>(&sz),
        current_offset_,
        sizeof(sz)
    );
    current_offset_ += sizeof(sz);

    switch (record.type) {
        case LogRecordType::INVALID_RECORD_TYPE: {
            throw std::runtime_error("Invalid record type encountered!");
        }
        case LogRecordType::BEGIN_RECORD:
        case LogRecordType::COMMIT_RECORD:
        case LogRecordType::ABORT_RECORD: {
            const TxnLogRecord& txn_rec = static_cast<const TxnLogRecord&>(record);
            log_file_->write_block(
                reinterpret_cast<const char*>(&txn_rec.txn_id), current_offset_, sizeof(txn_rec.txn_id));
            current_offset_ += sizeof(txn_rec.txn_id);
            break;
        }

        case LogRecordType::UPDATE_RECORD: {
            const UpdateLogRecord& upd_rec = static_cast<const UpdateLogRecord&>(record);
            log_file_->write_block(
                reinterpret_cast<const char*>(&upd_rec.txn_id), current_offset_, sizeof(upd_rec.txn_id));
            current_offset_ += sizeof(upd_rec.txn_id);
            
            log_file_->write_block(
                reinterpret_cast<const char*>(&upd_rec.page_id), current_offset_, sizeof(upd_rec.page_id));
            current_offset_ += sizeof(upd_rec.page_id);
            
            log_file_->write_block(
                reinterpret_cast<const char*>(&upd_rec.length), current_offset_, sizeof(upd_rec.length));
            current_offset_ += sizeof(upd_rec.length);
            
            log_file_->write_block(
                reinterpret_cast<const char*>(&upd_rec.offset), current_offset_, sizeof(upd_rec.offset));
            current_offset_ += sizeof(upd_rec.offset);

            uint64_t before_size = upd_rec.before_img.size();
            log_file_->write_block(
                reinterpret_cast<const char*>(&before_size), current_offset_, sizeof(before_size));
            current_offset_ += sizeof(before_size);
            if (before_size > 0) {
                log_file_->write_block(
                    reinterpret_cast<const char*>(upd_rec.before_img.data()), current_offset_, before_size);
                current_offset_ += before_size;
            }

            uint64_t after_size = upd_rec.after_img.size();
            log_file_->write_block(
                reinterpret_cast<const char*>(&after_size), current_offset_, sizeof(after_size));
            current_offset_ += sizeof(after_size);

            if (after_size > 0) {
                log_file_->write_block(
                    reinterpret_cast<const char*>(upd_rec.after_img.data()), current_offset_, after_size);
                current_offset_ += after_size;
            }
            break;
        }

        case LogRecordType::CHECKPOINT_RECORD: {
            const CheckpointLogRecord& ckpt_rec = static_cast<const CheckpointLogRecord&>(record);

            uint64_t count = ckpt_rec.active_txns.size();
            log_file_->write_block(
                reinterpret_cast<const char*>(&count), current_offset_, sizeof(count));
            current_offset_ += sizeof(count);

            for (auto& kv : ckpt_rec.active_txns) {
                uint64_t txn_id = kv.first;
                uint64_t first_log_offset = kv.second;

                log_file_->write_block(
                    reinterpret_cast<const char*>(&txn_id), current_offset_, sizeof(txn_id));
                current_offset_ += sizeof(txn_id);

                log_file_->write_block(
                    reinterpret_cast<const char*>(&first_log_offset), current_offset_, sizeof(first_log_offset));
                current_offset_ += sizeof(first_log_offset);
            }
            break;
        }
    } // switch
}

std::unique_ptr<LogManager::LogRecordHeader> LogManager::read_next_log_record(uint64_t &offset) {
    // If we've already reached or passed the end of the file, no more records
    
    // std::cout << "read_next_log_record: " << offset << std::endl;
    
    const uint64_t file_size = log_file_->size();
    if (offset >= file_size) {
        return nullptr;
    }

    LogRecordType rec_type;
    log_file_->read_block(offset, sizeof(rec_type), reinterpret_cast<char*>(&rec_type));
    offset += sizeof(rec_type);

    // std::cout << "---- " << to_string(rec_type) << std::endl;

    uint64_t rec_size;
    log_file_->read_block(offset, sizeof(rec_size), reinterpret_cast<char*>(&rec_size));
    offset += sizeof(rec_size);


    // std::cout << "---- rec_size: " << rec_size << std::endl;

    if (rec_size < (sizeof(rec_type) + sizeof(rec_size))) {
        throw std::runtime_error("Corrupted log record: rec_size too small.");
    }

    size_t body_len = rec_size - (sizeof(rec_type) + sizeof(rec_size));
    std::vector<char> body_buf(body_len);
    if (body_len > 0) {
        if (offset + body_len > file_size) {
            throw std::runtime_error("Corrupted log: body extends past file size.");
        }
        log_file_->read_block(offset, body_len, body_buf.data());
        offset += body_len;
    }

    switch (rec_type) {
        case LogRecordType::BEGIN_RECORD:
        case LogRecordType::COMMIT_RECORD:
        case LogRecordType::ABORT_RECORD:
        {
            auto record = std::make_unique<TxnLogRecord>();
            record->type = rec_type;
            record->rec_size = rec_size;

            if (body_len != sizeof(record->txn_id)) {
                throw std::runtime_error("Corrupted TxnLogRecord body size.");
            }
            std::memcpy(&record->txn_id, body_buf.data(), sizeof(record->txn_id));

            return record;
        }
        case LogRecordType::UPDATE_RECORD:
        {
            auto record = std::make_unique<UpdateLogRecord>();
            record->type = rec_type;
            record->rec_size = rec_size;

            size_t cursor = 0;
            auto read_u64 = [&](uint64_t &val) {
                std::memcpy(&val, &body_buf[cursor], sizeof(uint64_t));
                cursor += sizeof(uint64_t);
            };

            read_u64(record->txn_id);
            read_u64(record->page_id);
            read_u64(record->length);
            read_u64(record->offset);

            uint64_t before_size;
            read_u64(before_size);
            if (cursor + before_size > body_buf.size()) {
                throw std::runtime_error("Corrupted update record: before_img out of range.");
            }
            record->before_img.resize(before_size);
            if (before_size > 0) {
                std::memcpy(record->before_img.data(), &body_buf[cursor], before_size);
                cursor += before_size;
            }

            uint64_t after_size;
            read_u64(after_size);
            if (cursor + after_size > body_buf.size()) {
                throw std::runtime_error("Corrupted update record: after_img out of range.");
            }
            record->after_img.resize(after_size);
            if (after_size > 0) {
                std::memcpy(record->after_img.data(), &body_buf[cursor], after_size);
                cursor += after_size;
            }

            return record;
        }
        case LogRecordType::CHECKPOINT_RECORD:
        {
            auto record = std::make_unique<CheckpointLogRecord>();
            record->type = rec_type;
            record->rec_size = rec_size;

            size_t cursor = 0;
            auto read_u64 = [&](uint64_t &val) {
                std::memcpy(&val, &body_buf[cursor], sizeof(uint64_t));
                cursor += sizeof(uint64_t);
            };

            uint64_t count;
            read_u64(count);
            record->active_txns.reserve(count);

            for (uint64_t i = 0; i < count; i++) {
                uint64_t txn_id;
                uint64_t first_log_offset;
                read_u64(txn_id);
                read_u64(first_log_offset);

                record->active_txns.emplace_back(txn_id, first_log_offset);
            }

            return record;
        }
        default:
            throw std::runtime_error("Unknown or invalid log record type.");
    } // end switch
}

/**
 * Increment the BEGIN_RECORD count
 * Add the begin log record to the log file
 * Add to the active transactions
 */
void LogManager::log_txn_begin(UNUSED_ATTRIBUTE uint64_t txn_id) {

    std::cout << "[LSN " << get_total_log_records() << "] log_txn_begin (txn_id=" << txn_id << ")" << std::endl;

    log_record_type_to_count[LogRecordType::BEGIN_RECORD]++;

    txn_id_to_first_log_record[txn_id] = current_offset_;

    TxnLogRecord record(LogRecordType::BEGIN_RECORD, txn_id);
    write_log_record(record);
    
    active_txns.insert(txn_id);
}

/**
 * Increment the UPDATE_RECORD count
 * Add the update log record to the log file
 * @param txn_id		transaction id
 * @param page_id		buffer page id
 * @param length		length of the update tuple
 * @param offset 		offset to the tuple in the buffer page
 * @param before_img	before image of the buffer page at the given offset
 * @param after_img		after image of the buffer page at the given offset
 */
void LogManager::log_update(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE uint64_t page_id,
                            UNUSED_ATTRIBUTE uint64_t length, UNUSED_ATTRIBUTE uint64_t offset,
                            UNUSED_ATTRIBUTE std::byte* before_img,
                            UNUSED_ATTRIBUTE std::byte* after_img) {
    
    std::cout << "[LSN " << get_total_log_records() << "] log_update (txn_id=" << txn_id << ")" << std::endl;

    log_record_type_to_count[LogRecordType::UPDATE_RECORD]++;
    
    UpdateLogRecord record(txn_id, page_id, length, offset, before_img, after_img);
    write_log_record(record);
}

/**
 * Increment the COMMIT_RECORD count
 * Add commit log record to the log file
 * Remove from the active transactions
 */
void LogManager::log_commit(UNUSED_ATTRIBUTE uint64_t txn_id) {
    
    std::cout << "[LSN " << get_total_log_records() << "] log_commit (txn_id=" << txn_id << ")" << std::endl;

    log_record_type_to_count[LogRecordType::COMMIT_RECORD]++;

    TxnLogRecord record(LogRecordType::COMMIT_RECORD, txn_id);
    write_log_record(record);

    auto it = active_txns.find(txn_id);
    if (it != active_txns.end()) {
        active_txns.erase(it);
    }

    // TENTATIVE
    txn_id_to_first_log_record.erase(txn_id);
}

/**
 * Increment the ABORT_RECORD count.
 * Rollback the provided transaction.
 * Add abort log record to the log file.
 * Remove from the active transactions.
 */
void LogManager::log_abort(UNUSED_ATTRIBUTE uint64_t txn_id,
                           UNUSED_ATTRIBUTE BufferManager& buffer_manager) {
    
    std::cout << "[LSN " << get_total_log_records() << "] log_abort (txn_id=" << txn_id << ")" << std::endl;

    log_record_type_to_count[LogRecordType::ABORT_RECORD]++;

    rollback_txn(txn_id, buffer_manager);

    TxnLogRecord record(LogRecordType::ABORT_RECORD, txn_id);
    write_log_record(record);

    auto it = active_txns.find(txn_id);
    if (it != active_txns.end()) {
        active_txns.erase(it);
    }
    
    txn_id_to_first_log_record.erase(txn_id);
}

/**
 * Increment the CHECKPOINT_RECORD count
 * Flush all dirty pages to the disk (USE: buffer_manager.flush_all_pages())
 * Add the checkpoint log record to the log file
 */
void LogManager::log_checkpoint(UNUSED_ATTRIBUTE BufferManager& buffer_manager) {
    
    std::cout << "[LSN " << get_total_log_records() << "] log_checkpoint..." << std::endl;

    log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD]++;

    buffer_manager.flush_all_pages();

    CheckpointLogRecord ckpt_record;
    for (auto txn_id : active_txns) {
        auto it = txn_id_to_first_log_record.find(txn_id);
        uint64_t first_offset = (it != txn_id_to_first_log_record.end())
                                ? it->second
                                : 0;  // or some sentinel if not found
        ckpt_record.push_back(txn_id, first_offset);
    }

    write_log_record(ckpt_record);    
}

/**
 * @Analysis Phase:
 * 		1. Get the active transactions and commited transactions
 * 		2. Restore the txn_id_to_first_log_record
 * @Redo Phase:
 * 		1. Redo the entire log tape to restore the buffer page
 * 		2. For UPDATE logs: write the after_img to the buffer page
 * 		3. For ABORT logs: rollback the transactions
 * 	@Undo Phase
 * 		1. Rollback the transactions which are active and not commited
 */
void LogManager::recovery(UNUSED_ATTRIBUTE BufferManager& buffer_manager) {

    std::cout << "@@@@@@ recovery @@@@@@" << std::endl;

    std::unordered_set<uint64_t> active_txns_local;    // local set of active txns
    std::unordered_set<uint64_t> committed_txns_local; // local set of committed txns

    std::cout << "analysis_..." << std::endl;
    analysis_(active_txns_local, committed_txns_local);

    std::cout << "redo_..." << std::endl;
    redo_(buffer_manager);

    std::cout << "undo_..." << std::endl;
    undo_(buffer_manager, active_txns_local);
}

void LogManager::analysis_(
    std::unordered_set<uint64_t>& active_txns_local,
    std::unordered_set<uint64_t>& committed_txns_local) {
    
    txn_id_to_first_log_record.clear();
    active_txns_local.clear();
    committed_txns_local.clear();

    uint64_t offset = 0;
    uint64_t log_size = log_file_->size();

    while (offset < log_size) {
        auto log_record = read_next_log_record(offset);
        if (!log_record) {
            // No more records
            break;
        }

        // std::cout << to_string(log_record->type) << std::endl;

        switch (log_record->type) {
            case LogRecordType::BEGIN_RECORD: {
                auto *r = dynamic_cast<TxnLogRecord*>(log_record.get());
                active_txns_local.insert(r->txn_id);
                txn_id_to_first_log_record[r->txn_id] = offset - r->rec_size; // or however you track it
                break;
            }
            case LogRecordType::COMMIT_RECORD:
            case LogRecordType::ABORT_RECORD: {
                auto *r = dynamic_cast<TxnLogRecord*>(log_record.get());
                if (log_record->type == LogRecordType::COMMIT_RECORD) {
                    committed_txns_local.insert(r->txn_id);
                }

                // either way, remove from active txns
                active_txns_local.erase(r->txn_id);
                // txn_id_to_first_log_record.erase(r->txn_id);
                break;
            }
            
            case LogRecordType::UPDATE_RECORD: {
                /// For UPDATE_RECORD, we don't need to do anything in the analysis phase
                break;
            }
            
            case LogRecordType::CHECKPOINT_RECORD: {
                // For CHECKPOINT_RECORD, we don't need to do anything in the analysis phase
                break;
            }

            case LogRecordType::INVALID_RECORD_TYPE:
            default:
                throw std::runtime_error("Unknown or invalid record type encountered");
        } // switch
    } // while offset < log_size

}


void LogManager::redo_(
        BufferManager& buffer_manager) {

    uint64_t offset = 0;

    while (true) {
        std::unique_ptr<LogRecordHeader> log_record = read_next_log_record(offset);
        if (!log_record) {
            break;
        }

        switch (log_record->type) {
            case LogRecordType::BEGIN_RECORD:
            case LogRecordType::COMMIT_RECORD: {
                // no-op
                break;
            }
            // For ABORT_RECORD, rollback the transaction
            case LogRecordType::ABORT_RECORD: {
                auto* txn_rec = dynamic_cast<TxnLogRecord*>(log_record.get());
                
                rollback_txn(txn_rec->txn_id, buffer_manager);
                break;
            }
            // For UPDATE_RECORD, write the after_img to the buffer page
            case LogRecordType::UPDATE_RECORD: {
                auto* upd_rec = dynamic_cast<UpdateLogRecord*>(log_record.get());
                
                BufferFrame& frame = buffer_manager.fix_page(upd_rec->page_id, true);
                std::memcpy(&frame.get_data()[upd_rec->offset], upd_rec->after_img.data(), upd_rec->after_img.size());
                buffer_manager.unfix_page(frame, true);
                break;
            }
            case LogRecordType::CHECKPOINT_RECORD: {
                // no-op
                break;
            }
            case LogRecordType::INVALID_RECORD_TYPE:
            default:
                throw std::runtime_error("Unknown or invalid log record type encountered in redo.");
        }
    } // end while
}

void LogManager::undo_(
    BufferManager& buffer_manager,
    const std::unordered_set<uint64_t>& active_txns_local) {

    for (auto txn_id : active_txns_local) {
        rollback_txn(txn_id, buffer_manager);
    }
}

/**
 * Use txn_id_to_first_log_record to get the begin of the current transaction
 * Walk through the log tape and rollback the changes by writing the before
 * image of the tuple on the buffer page.
 * Note: There might be other transactions' log records interleaved, so be careful to
 * only undo the changes corresponding to current transactions.
 */
void LogManager::rollback_txn(uint64_t txn_id, BufferManager& buffer_manager) {
    
    // std::cout << "rollback_txn..." << " txn_id: " << txn_id << std::endl;

    auto it = txn_id_to_first_log_record.find(txn_id);
    if (it == txn_id_to_first_log_record.end()) {
        return;
    }
    uint64_t offset   = it->second;
    uint64_t log_size = log_file_->size();

    std::vector<std::unique_ptr<LogRecordHeader>> records;
    records.reserve(128);

    // std::cout << "offset: " << offset << ", log_size: " << log_size << std::endl;

    while (offset < log_size) {
        auto log_record = read_next_log_record(offset);

        // std::cout << to_string(log_record->type) << std::endl;

        if (!log_record) {
            break;
        }

        switch (log_record->type) {
            case LogRecordType::BEGIN_RECORD:
            case LogRecordType::COMMIT_RECORD: {
                auto* txn_rec = dynamic_cast<TxnLogRecord*>(log_record.get());
                // std::cout << "txn_rec->txn_id: " << txn_rec->txn_id << ", txn_id: " << txn_id << std::endl;
                if (txn_rec->txn_id == txn_id) {
                    records.push_back(std::move(log_record));
                }
                break;
            }
            case LogRecordType::ABORT_RECORD: {
                break;
            }
            case LogRecordType::UPDATE_RECORD: {
                auto* upd_rec = dynamic_cast<UpdateLogRecord*>(log_record.get());
                // std::cout << "upd_rec->txn_id: " << upd_rec->txn_id << ", txn_id: " << txn_id << std::endl;

                if (upd_rec->txn_id == txn_id) {
                    records.push_back(std::move(log_record));
                }
                break;
            }
            case LogRecordType::CHECKPOINT_RECORD: {
                break;
            }
            case LogRecordType::INVALID_RECORD_TYPE:
            default:
                throw std::runtime_error(
                    "rollback_txn: Unknown or invalid record type."
                );
        } // switch
    } // while offset < log_size

    // std::cout << "now undoing..." << std::endl;

    // now apply UNDO in reverse order
    for (int i = static_cast<int>(records.size()) - 1; i >= 0; i--) {
        std::unique_ptr<LogRecordHeader> &rec_ptr = records[i];

        // std::cout << to_string(rec_ptr->type) << std::endl;

        switch (rec_ptr->type) {
            case LogRecordType::BEGIN_RECORD: {
                // BEGIN_RECORD => done undoing
                return;   
            }
            case LogRecordType::COMMIT_RECORD:
            case LogRecordType::ABORT_RECORD: {
                // COMMIT_RECORD or ABORT_RECORD => we can stop as well
                return;
            }
            case LogRecordType::UPDATE_RECORD: {
                auto* upd_rec = dynamic_cast<UpdateLogRecord*>(rec_ptr.get());

                BufferFrame &frame = buffer_manager.fix_page(upd_rec->page_id, true);
                memcpy(&frame.get_data()[upd_rec->offset], upd_rec->before_img.data(), upd_rec->before_img.size());
                buffer_manager.unfix_page(frame, true);

                break;
            }
            default:
                break;
        } // switch
    }
}
}// namespace buzzdb