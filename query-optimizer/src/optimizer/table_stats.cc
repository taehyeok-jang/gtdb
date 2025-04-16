
#include "optimizer/table_stats.h"

#include <cmath>

namespace buzzdb {
namespace table_stats {

/**
 * Create a new IntHistogram.
 *
 * This IntHistogram should maintain a histogram of integer values that it receives.
 * It should split the histogram into "buckets" buckets.
 *
 * The values that are being histogrammed will be provided one-at-a-time through the "add_value()"
 * function.
 *
 * Your implementation should use space and have execution time that are both
 * constant with respect to the number of values being histogrammed.  For example, you shouldn't
 * simply store every value that you see in a sorted list.
 *
 * @param buckets The number of buckets to split the input value into.
 * @param min_val The minimum integer value that will ever be passed to this class for histogramming
 * @param max_val The maximum integer value that will ever be passed to this class for histogramming
 */
IntHistogram::IntHistogram(int64_t buckets, int64_t min_val, int64_t max_val) {
    assert(min_val <= max_val);

    buckets_ = static_cast<int>(buckets > 1 ? buckets : 1);
    min_ = min_val;
    max_ = max_val;

    width_ = ceil(static_cast<double>(max_val - min_val + 1) / 
                    static_cast<double>(buckets > 1 ? buckets : 1));
    counts_.resize(static_cast<size_t>(buckets > 1 ? buckets : 1), 0);
    ntups_ = 0;
}

IntHistogram::IntHistogram(const IntHistogram& other)
    : buckets_(other.buckets_),
      min_(other.min_),
      max_(other.max_),
      width_(other.width_),
      counts_(other.counts_),
      ntups_(other.ntups_) {
}

/**
 * Add a value to the set of values that you are keeping a histogram of.
 * @param val Value to add to the histogram
 */
void IntHistogram::add_value(int64_t val) {
    if (val < min_ || val > max_) {
        return;
    }

    int idx = bucket_index(val);
    counts_[idx]++;
    ntups_++;
}

/**
 * Estimate the selectivity of a particular predicate and operand on this table.
 *
 * For example, if "op" is "GREATER_THAN" and "v" is 5,
 * return your estimate of the fraction of elements that are greater than 5.
 *
 * @param op Operator
 * @param v Value
 * @return Predicted selectivity of this particular operator and value
 */
double IntHistogram::estimate_selectivity(PredicateType op, int64_t v) {
    
    if (ntups_ == 0) return 0.0;                       // avoid divide‑by‑zero

    auto eq_sel = [&]() -> double {
        if (v < min_ || v > max_) return 0.0;
        int idx   = bucket_index(v);
        double h  = static_cast<double>(counts_[idx]);
        return (h / bucket_width()) / static_cast<double>(ntups_);
    };

    auto lt_sel = [&]() -> double {
        if (v <= min_) return 0.0;
        if (v >  max_) return 1.0;
        int idx = bucket_index(v);
        double sel = 0.0;

        // complete buckets strictly below idx
        for (int i = 0; i < idx; ++i)
            sel += static_cast<double>(counts_[i]);

        // partial bucket that contains v
        double b_start = bucket_start(idx);
        double fraction = (static_cast<double>(v) - b_start) / bucket_width();
        sel += fraction * static_cast<double>(counts_[idx]);

        return sel / static_cast<double>(ntups_);
    };

    switch (op) {
        case PredicateType::EQ: return eq_sel();
        case PredicateType::NE: return 1.0 - eq_sel();
        case PredicateType::LT: return lt_sel();
        case PredicateType::LE: return lt_sel() + eq_sel();
        case PredicateType::GT: return 1.0 - lt_sel() - eq_sel();
        case PredicateType::GE: return 1.0 - lt_sel();
        default:                return 0.0;              // unsupported operator
    }
}

/**
 * Create a new TableStats object, that keeps track of statistics on each
 * column of a table
 *
 * @param table_id
 *            The table over which to compute statistics
 * @param io_cost_per_page
 *            The cost per page of IO. This doesn't differentiate between
 *            sequential-scan IO and disk seeks.
 * @param num_pages
 *            The number of disk pages spanned by the table
 * @param num_fields
 *            The number of columns in the table
 */
TableStats::TableStats(int64_t table_id, int64_t io_cost_per_page,
                    uint64_t num_pages, uint64_t num_fields) {

    table_id_ = table_id;
    io_cost_per_page_ = io_cost_per_page;
    num_pages_ = num_pages;
    num_fields_ = num_fields;
    total_tuples_ = 0;
    int_hists_.resize(num_fields_);

    std::vector<int64_t> mins(num_fields_, std::numeric_limits<int64_t>::max());
    std::vector<int64_t> maxs(num_fields_, std::numeric_limits<int64_t>::lowest());

    // scan to determine min/max values for each column
    operators::SeqScan scan1(static_cast<uint16_t>(table_id_), num_pages_, num_fields_);
    scan1.open();
    while (scan1.has_next()) {
        const std::vector<int>& tup = scan1.get_tuple();
        ++total_tuples_;

        for (uint64_t i = 0; i < num_fields_; ++i) {
            int64_t v   = static_cast<int64_t>(tup[i]);
            mins[i]     = std::min(mins[i], v);
            maxs[i]     = std::max(maxs[i], v);
        }
    }
    scan1.close();

    // build histograms for each column
    for (uint64_t i = 0; i < num_fields_; ++i) {
        if (mins[i] <= maxs[i]) {
            int_hists_[i] = std::make_unique<IntHistogram>(NUM_HIST_BINS, mins[i], maxs[i]);
        }
    }
  
    // populate histograms
    operators::SeqScan scan2(static_cast<uint16_t>(table_id_), num_pages_, num_fields_);
    scan2.open();
    while (scan2.has_next()) {
        const std::vector<int>& tup = scan2.get_tuple();
        for (uint64_t i = 0; i < num_fields_; ++i) {
            if (int_hists_[i]) int_hists_[i]->add_value(static_cast<int64_t>(tup[i]));
        }
    }
    scan2.close();
}

TableStats::TableStats(const TableStats& other) 
    : table_id_(other.table_id_),
      io_cost_per_page_(other.io_cost_per_page_),
      num_pages_(other.num_pages_),
      num_fields_(other.num_fields_),
      total_tuples_(other.total_tuples_) {
    
    // unique_ptr by deep copy 
    int_hists_.reserve(other.int_hists_.size());
    for (const auto& hist_ptr : other.int_hists_) {
        if (hist_ptr) {
            int_hists_.push_back(std::make_unique<IntHistogram>(*hist_ptr));
        } else {
            int_hists_.push_back(nullptr);
        }
    }
}

TableStats& TableStats::operator=(const TableStats& other) {
    if (this != &other) {
        table_id_ = other.table_id_;
        io_cost_per_page_ = other.io_cost_per_page_;
        num_pages_ = other.num_pages_;
        num_fields_ = other.num_fields_;
        total_tuples_ = other.total_tuples_;
        
        // unique_ptr 벡터 깊은 복사
        int_hists_.clear();
        int_hists_.reserve(other.int_hists_.size());
        for (const auto& hist_ptr : other.int_hists_) {
            if (hist_ptr) {
                int_hists_.push_back(std::make_unique<IntHistogram>(*hist_ptr));
            } else {
                int_hists_.push_back(nullptr);
            }
        }
    }
    return *this;
}

/**
 * Estimates the cost of sequentially scanning the file, given that the cost
 * to read a page is io_cost_per_page. You can assume that there are no seeks
 * and that no pages are in the buffer pool.
 *
 * Also, assume that your hard drive can only read entire pages at once, so
 * if the last page of the table only has one tuple on it, it's just as
 * expensive to read as a full page. (Most real hard drives can't
 * efficiently address regions smaller than a page at a time.)
 *
 * @return The estimated cost of scanning the table.
 */

double TableStats::estimate_scan_cost() { 
    return static_cast<double>(num_pages_) * io_cost_per_page_;
}

/**
 * This method returns the number of tuples in the relation, given that a
 * predicate with selectivity selectivity_factor is applied.
 *
 * @param selectivityFactor
 *            The selectivity of any predicates over the table
 * @return The estimated cardinality of the scan with the specified
 *         selectivity_factor
 */
uint64_t TableStats::estimate_table_cardinality(double selectivity_factor) {
    return static_cast<uint64_t>(static_cast<double>(total_tuples_) * selectivity_factor);
    // return static_cast<uint64_t>(std::round(static_cast<double>(total_tuples_) * selectivity_factor));
}

/**
 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
 * table.
 *
 * @param field
 *            The field over which the predicate ranges
 * @param op
 *            The logical operation in the predicate
 * @param constant
 *            The value against which the field is compared
 * @return The estimated selectivity (fraction of tuples that satisfy) the
 *         predicate
 */
double TableStats::estimate_selectivity(UNUSED_ATTRIBUTE int64_t field,
                                        UNUSED_ATTRIBUTE PredicateType op,
                                        UNUSED_ATTRIBUTE int64_t constant) {

    if (field < 0 || static_cast<uint64_t>(field) >= num_fields_) {
        return 1.0;
    }
    IntHistogram* hist_ptr = int_hists_[field].get();
    if (!hist_ptr) {
        return 1.0;
    }

    // Estimate selectivity using histogram
    return hist_ptr->estimate_selectivity(op, constant);
}

}  // namespace table_stats
}  // namespace buzzdb