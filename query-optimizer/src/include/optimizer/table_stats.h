#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

#include "operators/seq_scan.h"

using buzzdb::operators::PredicateType;

namespace buzzdb {
namespace table_stats {

class IntHistogram {
    public:
        IntHistogram() = default;
        IntHistogram(int64_t buckets, int64_t min_val, int64_t max_val);
        IntHistogram(const IntHistogram& other);

        double estimate_selectivity(PredicateType op, int64_t v);
        void add_value(int64_t val);

    private:
        inline double bucket_width() const { return width_; }
        inline int    bucket_index(int64_t v) const { // clamp 
            int idx = static_cast<int>((v - min_) / width_);
            if (idx < 0) return 0;
            if (idx >= buckets_) return buckets_ - 1;
            return idx;
        }
        inline double bucket_start(int idx) const { return min_ + idx * width_; }
        inline double bucket_end(int idx)   const { return bucket_start(idx) + width_; }

        int64_t              buckets_;
        int64_t              min_;
        int64_t              max_;
        double               width_;
        std::vector<int64_t> counts_;
        int64_t              ntups_{0};
};

class TableStats {
    public:
        TableStats() = default;
        TableStats(int64_t table_id, int64_t io_cost_per_page, uint64_t num_pages, uint64_t num_fields);
        TableStats(const TableStats& other);

        double estimate_selectivity(int64_t field, PredicateType op, int64_t constant);
        double estimate_scan_cost();
        uint64_t estimate_table_cardinality(double selectivity_factor);
        TableStats& operator=(const TableStats& other);

    private:
        /**
         * Number of bins for the histogram. Feel free to increase this value over
         * 100, though our tests assume that you have at least 100 bins in your
         * histograms.
         */
        int NUM_HIST_BINS = 100;

        int64_t     table_id_;
        int64_t     io_cost_per_page_;
        uint64_t    num_pages_;
        uint64_t    num_fields_;
        uint64_t    total_tuples_;
        std::vector<std::unique_ptr<IntHistogram>> int_hists_;
};

}  // namespace table_stats
}  // namespace buzzdb