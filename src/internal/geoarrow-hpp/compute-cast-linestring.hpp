
#pragma once

#include "handler.hpp"
#include "compute-builder.hpp"
#include "compute-cast-point.hpp"
#include "../arrow-hpp/builder.hpp"
#include "../arrow-hpp/builder-list.hpp"

namespace geoarrow {

class LinestringArrayBuilder: public ComputeBuilder {
public:
    LinestringArrayBuilder() {
        builder_.child().set_name("vertices");
    }

    void new_dimensions(util::Dimensions dimensions) {
        builder_.child().new_dimensions(dimensions);
    }

    Result null_feat() {
        size_++;
        builder_.finish_element(false);
        return Result::ABORT_FEATURE;
    }

    Result geom_start(util::GeometryType geometry_type, int32_t size) {
        if (size == 0) {
            size_++;
            builder_.finish_element();
            return Result::ABORT_FEATURE;
        } else if (geometry_type == util::GeometryType::LINESTRING) {
            if (size > 0) {
                builder_.child().reserve(size);
            }
            return Result::CONTINUE;
        } else if (size == 1 && geometry_type == util::GeometryType::MULTILINESTRING) {
            return Result::CONTINUE;
        } else {
            throw util::IOException("Can't write non-linestring (%d[%d]) as LINESTRING", geometry_type, size);
        }
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        return builder_.child().coords(coord, n, coord_size);
    }

    Result feat_end() {
        size_++;
        builder_.finish_element();
        return Result::CONTINUE;
    }

    void shrink() {
        ArrayBuilder::shrink();
        builder_.shrink();
    }

    void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
        shrink();
        builder_.release(array_data, schema);
    }

private:
    arrow::hpp::builder::ListArrayBuilder<PointArrayBuilder> builder_;
};

}
