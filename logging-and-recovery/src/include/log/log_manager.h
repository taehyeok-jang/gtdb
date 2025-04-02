#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <unordered_set>

#include "buffer/buffer_manager.h"
#include "storage/test_file.h"

namespace buzzdb {

class LogManager {
   public:
    enum class LogRecordType {
        INVALID_RECORD_TYPE,
        ABORT_RECORD,
        COMMIT_RECORD,
        UPDATE_RECORD,
        BEGIN_RECORD,
        CHECKPOINT_RECORD
    };

    struct LogRecordHeader {
        LogManager::LogRecordType type;
        uint64_t rec_size = 0; // storing the overall size in bytes.

        virtual ~LogRecordHeader() = default;
    };

    struct TxnLogRecord : public LogRecordHeader {
        uint64_t txn_id;

        TxnLogRecord() {
            this->type = LogRecordType::INVALID_RECORD_TYPE; 
            this->rec_size = 0;
            this->txn_id = 0;
        }

        TxnLogRecord(LogManager::LogRecordType t, uint64_t txnId)
        {
            this->type   = t;
            this->txn_id = txnId;
            this->rec_size = sizeof(this->type) + sizeof(this->rec_size) + sizeof(this->txn_id);
        }
    };

    struct UpdateLogRecord : public LogRecordHeader {
        uint64_t txn_id;
        uint64_t page_id;
        uint64_t length;
        uint64_t offset;
        std::vector<std::byte> before_img;
        std::vector<std::byte> after_img;

        UpdateLogRecord() {
            this->type = LogManager::LogRecordType::UPDATE_RECORD;
        }

        UpdateLogRecord(uint64_t txn_id, uint64_t page_id, uint64_t length, uint64_t offset,
                        std::byte* before_img_data, std::byte* after_img_data) {
            this->type = LogManager::LogRecordType::UPDATE_RECORD;
            this->txn_id = txn_id;
            this->page_id = page_id;
            this->length = length;
            this->offset = offset;
            
            before_img.assign(before_img_data, before_img_data + length);
            after_img.assign(after_img_data, after_img_data + length);
            
            this->rec_size =
                sizeof(this->type) + sizeof(this->rec_size) +
                sizeof(this->txn_id) + sizeof(this->page_id) + sizeof(this->length) + sizeof(this->offset) +
                sizeof(uint64_t) + before_img.size() + sizeof(uint64_t) + after_img.size();
        }
    };

    struct CheckpointLogRecord : public LogRecordHeader {
        // Each pair is (active_txn_id, first_log_record_offset)
        std::vector<std::pair<uint64_t, uint64_t>> active_txns;

        CheckpointLogRecord() {
            this->type = LogManager::LogRecordType::CHECKPOINT_RECORD;
            this->rec_size = (sizeof(this->type) + sizeof(this->rec_size) + sizeof(uint64_t));
        }

        void push_back(uint64_t txn_id, uint64_t first_log_record_offset) {
            active_txns.push_back(std::make_pair(txn_id, first_log_record_offset));
            this->rec_size += 2 * sizeof(uint64_t);
        }
    };

    /// Constructor.
    LogManager(File* log_file);

    /// Destructor.
    ~LogManager();


    /// Add a txn begin record
    void log_txn_begin(uint64_t txn_id);

    /// Add an update record
    void log_update(uint64_t txn_id, uint64_t page_id, uint64_t length, uint64_t offset,
                    std::byte* before_img, std::byte* after_img);

    /// Add a commit record
    void log_commit(uint64_t txn_id);

    /// Add an abort record
    void log_abort(uint64_t txn_id, BufferManager& buffer_manager);

    /// Add a log checkpoint record
    void log_checkpoint(BufferManager& buffer_manager);

    /// recovery
    void recovery(BufferManager& buffer_manager);

    /// rollback a txn
    void rollback_txn(uint64_t txn_id, BufferManager& buffer_manager);

    /// Get log records
    uint64_t get_total_log_records();

    /// Get log records of a given type
    uint64_t get_total_log_records_of_type(LogRecordType type);

    /// reset the state, used to simulate crash
    void reset(File* log_file);

   private:
    File* log_file_;

    // offset in the file
    size_t current_offset_ = 0;

    std::map<uint64_t, uint64_t> txn_id_to_first_log_record;
    // std::map<uint64_t, uint64_t> txn_id_to_last_log_record;

    std::map<LogRecordType, uint64_t> log_record_type_to_count;

    // ATT (active transaction table)
    std::set<uint64_t> active_txns;

    // A helper function for writing log records
    void write_log_record(const LogRecordHeader& record);

    /**
     * Reads the next log record from the given file offset.
     * 
     * @param offset   [IN/OUT] The current offset in the log file. Will be advanced
     *                  by the size of the record that was read.
     * @return A unique_ptr to a LogRecordHeader, which can be one of:
     *         - TxnLogRecord
     *         - UpdateLogRecord
     *         - CheckpointLogRecord
     *         or nullptr if offset >= file size (no more records to read).
     */
    std::unique_ptr<LogRecordHeader> read_next_log_record(uint64_t &offset);

    void analysis_(
        std::unordered_set<uint64_t>& active_txns_local,
        std::unordered_set<uint64_t>& committed_txns_local);

    void redo_(
        BufferManager& buffer_manager);
    
    void undo_(
        BufferManager& buffer_manager,
        const std::unordered_set<uint64_t>& active_txns_local);
};

}  // namespace buzzdb
