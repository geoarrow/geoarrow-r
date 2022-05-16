
#pragma once

#include <algorithm>
#include <limits>
#include <memory>

#include "handler.hpp"
#include "compute-builder.hpp"
#include "internal/arrow-hpp/builder.hpp"
#include "internal/arrow-hpp/builder-struct.hpp"

namespace geoarrow {
namespace util {

class GenericBounder {
public:
    GenericBounder(Dimensions dim = Dimensions::XYZM): dim_(dim) {
        reset(4);
        xyzm_map_[0] = 0;
        xyzm_map_[1] = 1;
        xyzm_map_[2] = 2;
        xyzm_map_[3] = 3;

        if (dim_ == Dimensions::XYM) {
            xyzm_map_[2] = 3;
            xyzm_map_[3] = 2;
        }
    }

    void reset(int n) {
        for (int i = 0; i < n; i++) {
            min_values_[i] = std::numeric_limits<double>::infinity();
            max_values_[i] = -std::numeric_limits<double>::infinity();
        }
    }

    void add_coords(const double* coord, int64_t n, int32_t coord_size) {
        for (int64_t i = 0; i < n; i++) {
            for (int32_t j = 0; j < coord_size; j++) {
                double ordinate = coord[i * coord_size + j];
                min_values_[j] = std::min<double>(min_values_[j], ordinate);
                max_values_[j] = std::max<double>(max_values_[j], ordinate);
            }
        }
    }

    void add_bounder(const GenericBounder& other) {
        for (int j = 0; j < 4; j++) {
            double min_ordinate_xyzm = other.min_xyzm(j);
            double max_ordinate_xyzm = other.max_xyzm(j);
            int coord_j = xyzm_map_[j];
            min_values_[coord_j] = std::min<double>(min_values_[coord_j], min_ordinate_xyzm);
            max_values_[coord_j] = std::max<double>(max_values_[coord_j], max_ordinate_xyzm);
        }
    }

    void set_null() {
        for (int j = 0; j < 4; j++) {
            min_values_[j] = std::numeric_limits<double>::quiet_NaN();
            max_values_[j] = std::numeric_limits<double>::quiet_NaN();
        }
    }

    bool is_empty_coord(int dim) const {
        return (min_coord(dim) - max_coord(dim)) !=
            std::numeric_limits<double>::infinity();
    }

    bool is_empty_xyzm(int dim) const {
        return (min_xyzm(dim) - max_xyzm(dim)) !=
            std::numeric_limits<double>::infinity();
    }

    double min_coord(int dim) const { return min_values_[dim]; }
    double max_coord(int dim) const { return max_values_[dim]; }
    double min_xyzm(int dim) const { return min_values_[xyzm_map_[dim]]; }
    double max_xyzm(int dim) const { return max_values_[xyzm_map_[dim]]; }

private:
    Dimensions dim_;
    int xyzm_map_[4];
    double min_values_[4];
    double max_values_[4];
};

}

class GlobalBounder: public ComputeBuilder {
public:
    GlobalBounder(const ComputeOptions& options):
        bounder_xyzm_(util::Dimensions::XYZM),
        bounder_xym_(util::Dimensions::XYM),
        bounder_(nullptr) {

        null_is_empty_ = options.get_bool("null_is_empty");

        bounder_ = &bounder_xyzm_;
        for (int i = 0; i < 8; i++) {
            builders_[i] = std::unique_ptr<arrow::hpp::builder::Float64ArrayBuilder>(
                new arrow::hpp::builder::Float64ArrayBuilder());
        }
    }

    void new_dimensions(util::Dimensions dim) {
        if (dim == util::Dimensions::XYM) {
            bounder_ = &bounder_xym_;
        } else {
            bounder_ = &bounder_xyzm_;
        }
    }

    Result null_feat() {
        if (null_is_empty_) {
            return Result::CONTINUE;
        }

        bounder_xyzm_.set_null();
        bounder_xym_.set_null();
        return Result::ABORT;
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        bounder_->add_coords(coord, n, coord_size);
        return Result::CONTINUE;
    }

    void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
        // Combine the two bounders into one happy global bound
        bounder_xyzm_.add_bounder(bounder_xym_);

        // Build the Struct. Probably should be configurable to select
        // which dimensions are included at some point.
        arrow::hpp::builder::StructArrayBuilder builder;

        for (int i = 0; i < 4; i++) {
            builders_[i * 2]->write_element(bounder_xyzm_.min_xyzm(i));
            builders_[i * 2 + 1]->write_element(bounder_xyzm_.max_xyzm(i));
        }

        builder.add_child(std::move(builders_[0]), "xmin");
        builder.add_child(std::move(builders_[1]), "xmax");
        builder.add_child(std::move(builders_[2]), "ymin");
        builder.add_child(std::move(builders_[3]), "ymax");
        builder.add_child(std::move(builders_[4]), "zmin");
        builder.add_child(std::move(builders_[5]), "zmax");
        builder.add_child(std::move(builders_[6]), "mmin");
        builder.add_child(std::move(builders_[7]), "mmax");

        builder.shrink();
        builder.release(array_data, schema);
    }

private:
    bool null_is_empty_;
    util::GenericBounder bounder_xyzm_;
    util::GenericBounder bounder_xym_;
    util::GenericBounder* bounder_;
    std::unique_ptr<arrow::hpp::builder::Float64ArrayBuilder> builders_[8];
};

}
