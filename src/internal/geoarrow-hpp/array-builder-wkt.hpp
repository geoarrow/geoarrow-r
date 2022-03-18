
#pragma once

#include <cstring>
#include <cstdio>
#include <vector>

#include "handler.hpp"
#include "array-builder.hpp"

namespace geoarrow {

class WKTArrayBuilder: public GeoArrayBuilder {
public:
    WKTArrayBuilder(int64_t size = 1024, int64_t data_size_guess = 1024):
        string_builder_(size, data_size_guess),
        is_first_geom_(true),
        is_first_ring_(true),
        is_first_coord_(true) {
        stack_.reserve(32);
    }

    Result feat_start() {
        stack_.clear();
        is_first_geom_ = true;
        is_first_ring_ = true;
        is_first_coord_ = true;
        return Result::CONTINUE;
    }

    Result null_feat() {
        string_builder_.finish_element(false);
        return Result::ABORT_FEATURE;
    }

    Result geom_start(util::GeometryType geometry_type, int32_t size) {
        if (!is_first_geom_) {
            write_string(", ");
        } else {
            is_first_geom_ = false;
        }

        if (stack_.size() == 0 ||
                stack_.back().first == util::GeometryType::GEOMETRYCOLLECTION) {
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
        }

        if (size == 0) {
            write_string("EMPTY");
        } else {
            write_char('(');
        }

        stack_.push_back(std::pair<util::GeometryType, int32_t>(geometry_type, size));
        return Result::CONTINUE;
    }

    Result ring_start(int32_t size) {
        if (!is_first_ring_) {
            write_string(", ");
        } else {
            is_first_ring_ = false;
        }

        write_char('(');

        return Result::CONTINUE;
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        string_builder_.reserve_data(20 * n * coord_size);

        for (int64_t i = 0; i < n; i++) {
            if (!is_first_coord_) {
                write_string(", ");
            }

            write_coord(coord[i * coord_size]);
            for (int32_t j = 1; j < coord_size; j++) {
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
        if (stack_.size() > 0 || stack_.back().second > 0) {
            write_char(')');
            stack_.pop_back();
        }

        return Result::CONTINUE;
    }

    Result feat_end() {
        string_builder_.finish_element(true);
        return Result::CONTINUE;
    }

private:
    builder::StringArrayBuilder string_builder_;
    std::vector<std::pair<util::GeometryType, int32_t>> stack_;
    bool is_first_geom_;
    bool is_first_ring_;
    bool is_first_coord_;

    void write_coord(double value) {
        string_builder_.reserve_data(32);
        int64_t remaining = string_builder_.remaining_data_capacity();
        uint8_t* data_at_cursor = string_builder_.data_at_cursor(&remaining);
        int n_needed = snprintf(
            reinterpret_cast<char*>(data_at_cursor),
            remaining - 1, "%g", value);

        if (n_needed > remaining) {
            throw util::IOException(
                "Failed to reserve enough characters to write %g", value);
        }
    }

    void write_string(const char* value) {
        string_builder_.write_buffer(reinterpret_cast<const uint8_t*>(value), strlen(value));
    }

    void write_char(char value) {
        string_builder_.write_buffer(reinterpret_cast<const uint8_t*>(&value), 1);
    }
};

}
