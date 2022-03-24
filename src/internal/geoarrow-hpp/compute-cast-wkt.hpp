
#pragma once

#include <cstring>
#include <cstdio>
#include <vector>

#include "handler.hpp"
#include "compute-builder.hpp"
#include "../arrow-hpp/builder.hpp"
#include "../arrow-hpp/builder-string.hpp"

// using ryu for double -> char* is ~5x faster!
#ifndef geoarrow_d2s_fixed_n
static inline int geoarrow_compat_d2s_fixed_n(double f, uint32_t precision, char* result) {
    return snprintf(result, 128, "%.*g", precision, f);
}

#define geoarrow_d2s_fixed_n geoarrow_compat_d2s_fixed_n
#endif

namespace geoarrow {

class WKTArrayBuilder: public ComputeBuilder {
public:
    WKTArrayBuilder(int64_t size = 1024, int64_t data_size_guess = 1024):
        significant_digits_(16),
        string_builder_(size, data_size_guess),
        is_first_ring_(true),
        is_first_coord_(true),
        dimensions_(util::Dimensions::XY) {
        stack_.reserve(32);
    }

    void reserve(int64_t additional_capacity) {
        string_builder_.reserve(additional_capacity);
    }

    void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
        string_builder_.release(array_data, schema);
    }

    void new_dimensions(util::Dimensions dimensions) {
        dimensions_ = dimensions;
    }

    Result feat_start() {
        stack_.clear();
        is_first_ring_ = true;
        is_first_coord_ = true;
        return Result::CONTINUE;
    }

    Result null_feat() {
        string_builder_.finish_element(false);
        return Result::ABORT_FEATURE;
    }

    Result geom_start(util::GeometryType geometry_type, int32_t size) {
        if (stack_.size() > 0 && stack_.back().part > 0) {
            write_string(", ");
        }

        if (stack_.size() > 0) {
            stack_.back().part++;
        }

        if (stack_.size() == 0 ||
                stack_.back().type == util::GeometryType::GEOMETRYCOLLECTION) {
            switch (geometry_type) {
            case util::GeometryType::POINT:
                write_string("POINT");
                break;
            case util::GeometryType::LINESTRING:
                write_string("LINESTRING");
                break;
            case util::GeometryType::POLYGON:
                write_string("POLYGON");
                break;
            case util::GeometryType::MULTIPOINT:
                write_string("MULTIPOINT");
                break;
            case util::GeometryType::MULTILINESTRING:
                write_string("MULTILINESTRING");
                break;
            case util::GeometryType::MULTIPOLYGON:
                write_string("MULTIPOLYGON");
                break;
            case util::GeometryType::GEOMETRYCOLLECTION:
                write_string("GEOMETRYCOLLECTION");

                break;
            default:
                throw util::IOException(
                    "Don't know how to name geometry type %d",
                    geometry_type);
            }

            write_char(' ');

            switch (dimensions_) {
            case util::Dimensions::XYZM:
                write_string("ZM ");
                break;
            case util::Dimensions::XYZ:
                write_string("Z ");
                break;
            case util::Dimensions::XYM:
                write_string("M ");
                break;
            default:
                break;
            }
        }

        if (size == 0) {
            write_string("EMPTY");
        } else {
            write_char('(');
        }

        stack_.push_back(State{geometry_type, size, 0});
        is_first_ring_ = true;
        is_first_coord_ = true;
        return Result::CONTINUE;
    }

    Result ring_start(int32_t size) {
        if (!is_first_ring_) {
            write_string(", ");
        } else {
            is_first_ring_ = false;
        }

        write_char('(');

        is_first_coord_ = true;
        return Result::CONTINUE;
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        string_builder_.reserve_data((significant_digits_ + 4) * n * coord_size);

        for (int64_t i = 0; i < n; i++) {
            if (!is_first_coord_) {
                write_string(", ");
            }

            write_coord(coord[i * coord_size]);
            for (int32_t j = 1; j < coord_size; j++) {
                write_char(' ');
                write_coord(coord[i * coord_size + j]);
            }

            is_first_coord_ = false;
        }

        return Result::CONTINUE;
    }

    Result ring_end() {
        write_char(')');
        return Result::CONTINUE;
    }

    Result geom_end() {
        if (stack_.size() > 0 && stack_.back().size != 0) {
            write_char(')');
        }

        if (stack_.size() > 0) {
            stack_.pop_back();
        }

        return Result::CONTINUE;
    }

    Result feat_end() {
        string_builder_.finish_element(true);
        return Result::CONTINUE;
    }

private:
    class State {
    public:
        util::GeometryType type;
        int32_t size;
        int32_t part;
    };

    int significant_digits_;
    arrow::hpp::builder::StringArrayBuilder string_builder_;
    std::vector<State> stack_;
    bool is_first_ring_;
    bool is_first_coord_;
    util::Dimensions dimensions_;

    void write_coord(double value) {
        uint8_t* data_at_cursor = string_builder_.data_at_cursor();
        int n_needed = geoarrow_d2s_fixed_n(
            value,
            significant_digits_,
            reinterpret_cast<char*>(data_at_cursor));
        string_builder_.advance_data(n_needed);
    }

    void write_string(const char* value) {
        string_builder_.write_buffer(reinterpret_cast<const uint8_t*>(value), strlen(value));
    }

    void write_char(char value) {
        string_builder_.write_buffer(reinterpret_cast<const uint8_t*>(&value), 1);
    }
};

}
