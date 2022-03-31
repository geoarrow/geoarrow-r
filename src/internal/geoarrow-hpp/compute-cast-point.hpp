
#pragma once

#include <cmath>

#include "handler.hpp"
#include "compute-builder.hpp"
#include "../arrow-hpp/builder.hpp"
#include "../arrow-hpp/builder-list.hpp"

namespace geoarrow {

class PointArrayBuilder: public ComputeBuilder {
public:
    PointArrayBuilder():
      dimensions_(util::Dimensions::DIMENSIONS_UNKNOWN),
      builder_xy_(2),
      builder_xyz_(3),
      builder_xym_(3),
      builder_xyzm_(4),
      builder_(&builder_xy_) {
        builder_xy_.child().set_name("xy");
        builder_xyz_.child().set_name("xyz");
        builder_xym_.child().set_name("xym");
        builder_xyzm_.child().set_name("xyzm");
    }

    void reserve(int64_t additional_capacity) {
        builder_->reserve(additional_capacity);
    }

    void shrink() {
        ArrayBuilder::shrink();
        builder_->shrink();
    }

    void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
        shrink();
        builder_->release(array_data, schema);
    }

    const char* get_format() {
        return builder_->get_format();
    }

    void new_dimensions(util::Dimensions dimensions) {
        if (dimensions_ == util::Dimensions::DIMENSIONS_UNKNOWN) {
            switch (dimensions) {
            case util::Dimensions::XYZ:
                builder_ = &builder_xyz_;
                break;
            case util::Dimensions::XYM:
                builder_ = &builder_xym_;
                break;
            case util::Dimensions::XYZM:
                builder_ = &builder_xyzm_;
                break;
            default:
                builder_ = &builder_xy_;
                break;
            }
        } else if (dimensions != dimensions_) {
            throw util::IOException(
                "PointBuilder adapting to multiple dimensions not implemented");
        }

        dimensions_ = dimensions;
    }

    Result null_feat() {
        double empty_coord[] = {NAN, NAN, NAN, NAN};
        builder_->child().write_buffer(empty_coord, builder_->item_size());
        builder_->finish_elements(1, false);
        return Result::ABORT_FEATURE;
    }

    Result geom_start(util::GeometryType geometry_type, int32_t size) {
        if (size == 0) {
            double empty_coord[] = {NAN, NAN, NAN, NAN};
            coords(empty_coord, 1, builder_->item_size());
        } else if (size > 1 && geometry_type == util::GeometryType::MULTIPOINT) {
            throw util::IOException("Can't write MULTIPOINT[%d] as POINT", size);
        } else if (geometry_type != util::GeometryType::POINT) {
            throw util::IOException("Can't write non-point as POINT");
        }

        return Result::CONTINUE;
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        builder_->child().write_buffer(coord, n * coord_size);
        builder_->finish_elements(n);
        return Result::CONTINUE;
    }

private:
    using Float64ListBuilder =
        arrow::hpp::builder::FixedSizeListArrayBuilder<arrow::hpp::builder::Float64ArrayBuilder>;
    util::Dimensions dimensions_;
    Float64ListBuilder builder_xy_;
    Float64ListBuilder builder_xyz_;
    Float64ListBuilder builder_xym_;
    Float64ListBuilder builder_xyzm_;
    Float64ListBuilder* builder_;
};

}
