
#pragma once

#include <cstring>
#include <cstdio>
#include <vector>
#include <cmath>

#include "handler.hpp"
#include "compute-builder.hpp"
#include "../arrow-hpp/builder.hpp"
#include "../arrow-hpp/builder-string.hpp"

namespace geoarrow {

class WKBArrayBuilder: public ComputeBuilder {
public:
    WKBArrayBuilder(int64_t size = 1024, int64_t data_size_guess = 1024):
        endian_(0x01),
        string_builder_(size, data_size_guess),
        dimensions_(util::Dimensions::XY) {
        stack_.reserve(32);
    }

    void reserve(int64_t additional_capacity) {
        string_builder_.reserve(additional_capacity);
    }

    void shrink() {
        ArrayBuilder::shrink();
        string_builder_.shrink();
    }

    void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
        shrink();
        string_builder_.release(array_data, schema);
    }

    void new_dimensions(util::Dimensions dimensions) {
        dimensions_ = dimensions;
    }

    Result feat_start() {
        stack_.clear();
        return Result::CONTINUE;
    }

    Result null_feat() {
        string_builder_.finish_element(false);
        return Result::ABORT_FEATURE;
    }

    Result geom_start(util::GeometryType geometry_type, int32_t size) {
        if (stack_.size() > 0) {
            stack_.back().size++;
        }

        string_builder_.reserve_data(1 + 2 * sizeof(int32_t) + 2 * sizeof(double));
        memcpy(string_builder_.data_at_cursor(), &endian_, sizeof(uint8_t));
        string_builder_.advance_data(1);

        // the choices for the Dimensions Enum values were 100% fudged to
        // simplify this line of code
        uint32_t geometry_type_to_write = dimensions_ + geometry_type;
        memcpy(string_builder_.data_at_cursor(), &geometry_type_to_write, sizeof(uint32_t));
        string_builder_.advance_data(sizeof(uint32_t));

        // If we're reading WKT and maybe some future data source that also
        // doesn't have any information about what's coming, we can't write
        // the true size here. Instead, we save the address and write it when
        // the geom_end() / ring_end() methods are called.
        if (geometry_type != util::GeometryType::POINT) {
            uint32_t size_to_write = 0;
            memcpy(string_builder_.data_at_cursor(), &size_to_write, sizeof(int32_t));
            stack_.push_back(State{geometry_type, string_builder_.data_size(), 0});
            string_builder_.advance_data(sizeof(uint32_t));
        } else if (size == 0) {
            stack_.push_back(State{geometry_type, 0, 0});
            double empty_coord[] = {NAN, NAN, NAN, NAN};

            switch (dimensions_) {
            case util::Dimensions::XY:
                coords(empty_coord, 1, 2);
                break;
            case util::Dimensions::XYZ:
            case util::Dimensions::XYM:
                coords(empty_coord, 1, 3);
                break;
            default:
                coords(empty_coord, 1, 4);
                break;
            }
        } else {
            stack_.push_back(State{geometry_type, 0, 0});
        }

        return Result::CONTINUE;
    }

    Result ring_start(int32_t size) {
        if (stack_.size() > 0) {
            stack_.back().size++;
        }

        string_builder_.reserve_data(sizeof(uint32_t));

        uint32_t size_to_write = 0;
        memcpy(string_builder_.data_at_cursor(), &size_to_write, sizeof(uint32_t));
        stack_.push_back(State{util::GeometryType::LINESTRING, string_builder_.data_size(), 0});
        string_builder_.advance_data(sizeof(uint32_t));

        return Result::CONTINUE;
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        if (stack_.size() > 0) {
            stack_.back().size += n;
        }

        int64_t n_bytes = sizeof(double) * n * coord_size;
        string_builder_.reserve_data(n_bytes);
        memcpy(string_builder_.data_at_cursor(), coord, n_bytes);
        string_builder_.advance_data(n_bytes);

        return Result::CONTINUE;
    }

    Result ring_end() {
        if (stack_.size() > 0) {
            memcpy(
                string_builder_.mutable_data() + stack_.back().data_buffer_position,
                &stack_.back().size,
                sizeof(uint32_t));
            stack_.pop_back();
        }
        return Result::CONTINUE;
    }

    Result geom_end() {
        if (stack_.size() > 0 && stack_.back().geometry_type != util::GeometryType::POINT) {
            memcpy(
                string_builder_.mutable_data() + stack_.back().data_buffer_position,
                &stack_.back().size,
                sizeof(uint32_t));
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
        util::GeometryType geometry_type;
        int64_t data_buffer_position;
        uint32_t size;
    };

    uint8_t endian_;
    arrow::hpp::builder::BinaryArrayBuilder string_builder_;
    std::vector<State> stack_;
    util::Dimensions dimensions_;
};

}
