

#pragma once

#include "handler.hpp"
#include "schema.hpp"
#include "compute-builder.hpp"
#include "compute-cast-point.hpp"
#include "compute-cast-linestring.hpp"
#include "compute-cast-polygon.hpp"
#include "internal/arrow-hpp/builder.hpp"
#include "internal/arrow-hpp/builder-list.hpp"

namespace geoarrow {

template <typename Child, util::GeometryType ParentType, util::GeometryType ChildType>
class MultiArrayBuilder: public ComputeBuilder {
public:
    MultiArrayBuilder(const ComputeOptions& options = ComputeOptions()):
      ComputeBuilder(options), level_(0) {
        // Probably should live with subclasses
        switch (ParentType) {
        case util::GeometryType::MULTIPOINT:
            builder_.child().set_name("points");
            break;
        case util::GeometryType::MULTILINESTRING:
            builder_.child().set_name("linestrings");
            break;
        case util::GeometryType::MULTIPOLYGON:
            builder_.child().set_name("polygons");
            break;
        default:
            throw util::IOException("Unknown ParentType");
        }
    }

    void new_dimensions(util::Dimensions dimensions) {
        builder_.child().new_dimensions(dimensions);
    }

    Result feat_start() {
        level_ = 0;
        return Result::CONTINUE;
    }

    Result null_feat() {
        builder_.finish_element(false);
        return Result::ABORT_FEATURE;
    }

    Result ring_start(int32_t size) {
        return builder_.child().ring_start(size);
    }

    Result geom_start(util::GeometryType geometry_type, int32_t size) {
        level_++;
        if (level_ > 1) {
            builder_.child().feat_start();
            builder_.child().geom_start(geometry_type, size);
            return Result::CONTINUE;
        } else if (size == 0) {
            builder_.finish_element();
            return Result::ABORT_FEATURE;
        } if (geometry_type == ParentType) {
            if (size > 0) {
                builder_.reserve(size);
            }
            return Result::CONTINUE;
        } else if (geometry_type == ChildType) {
            level_++;
            builder_.child().geom_start(geometry_type, size);
            return Result::CONTINUE;
        } else {
            throw util::IOException("Can't write %d[%d] as %d", geometry_type, size, ParentType);
        }
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        return builder_.child().coords(coord, n, coord_size);
    }

    Result ring_end() {
        return builder_.child().ring_end();
    }

    Result geom_end() {
        level_--;
        if (level_ > 0) {
            builder_.child().geom_end();
            builder_.child().feat_end();
        }

        return Result::CONTINUE;
    }

    Result feat_end() {
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

        // Probably should live with subclasses
        switch (ParentType) {
        case util::GeometryType::MULTIPOINT:
            builder_.set_metadata("ARROW:extension:name", "geoarrow.multipoint");
            break;
        case util::GeometryType::MULTILINESTRING:
            builder_.set_metadata("ARROW:extension:name", "geoarrow.multilinestring");
            break;
        case util::GeometryType::MULTIPOLYGON:
            builder_.set_metadata("ARROW:extension:name", "geoarrow.multipolygon");
            break;
        default:
            throw util::IOException("Unknown ParentType");
        }

        builder_.set_metadata("ARROW:extension:metadata", Metadata().build());
        builder_.release(array_data, schema);

        // checks output and copies metadata
        finish_schema(schema);
    }

private:
    int level_;
    arrow::hpp::builder::ListArrayBuilder<Child> builder_;
};

using MultiPointArrayBuilder =
    MultiArrayBuilder<PointArrayBuilder, util::GeometryType::MULTIPOINT, util::GeometryType::POINT>;

using MultiLinestringArrayBuilder =
    MultiArrayBuilder<LinestringArrayBuilder, util::GeometryType::MULTILINESTRING, util::GeometryType::LINESTRING>;

using MultiPolygonArrayBuilder =
    MultiArrayBuilder<PolygonArrayBuilder, util::GeometryType::MULTIPOLYGON, util::GeometryType::POLYGON>;

}
