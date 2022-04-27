
#pragma once

#include "handler.hpp"
#include "schema.hpp"
#include "compute-builder.hpp"
#include "compute-cast-point.hpp"
#include "../arrow-hpp/builder.hpp"
#include "../arrow-hpp/builder-list.hpp"

namespace geoarrow {

class PolygonArrayBuilder: public ComputeBuilder {
public:
    PolygonArrayBuilder() {
        builder_.child().set_name("rings");
        builder_.child().child().set_name("vertices");
    }

    void new_dimensions(util::Dimensions dimensions) {
        builder_.child().child().new_dimensions(dimensions);
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
        } else if (geometry_type == util::GeometryType::POLYGON) {
            if (size > 0) {
                builder_.child().reserve(size);
            }
            return Result::CONTINUE;
        } else if (size == 1 && geometry_type == util::GeometryType::MULTIPOLYGON) {
            return Result::CONTINUE;
        } else {
            throw util::IOException("Can't write non-polygon (%d[%d]) as POLYGON", geometry_type, size);
        }
    }

    Result ring_start(int32_t size) {
        if (size > 0) {
            builder_.child().reserve(size);
        }

        return Result::CONTINUE;
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        return builder_.child().child().coords(coord, n, coord_size);
    }

    Result ring_end() {
        builder_.child().finish_element();
        return Result::CONTINUE;
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
        builder_.set_name(name());
        builder_.set_metadata("ARROW:extension:name", "geoarrow.polygon");
        builder_.set_metadata("ARROW:extension:metadata", Metadata().build());
        builder_.release(array_data, schema);
    }

private:
    arrow::hpp::builder::ListArrayBuilder<arrow::hpp::builder::ListArrayBuilder<PointArrayBuilder>> builder_;
};

}
