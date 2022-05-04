
#pragma once

#include <unordered_set>

#include "handler.hpp"
#include "compute-builder.hpp"
#include "../arrow-hpp/builder.hpp"
#include "../arrow-hpp/builder-string.hpp"

namespace geoarrow {

namespace {

struct type_dim_pair_hash {
    std::size_t operator() (const std::pair<util::GeometryType, util::Dimensions> &pair) const {
        return pair.first + pair.second;
    }
};

}

class GeoParquetTypeCollector: public ComputeBuilder {
public:
    GeoParquetTypeCollector(const ComputeOptions& options):
      include_empty_(true),
      dim_(util::Dimensions::DIMENSIONS_UNKNOWN),
      geometry_type_(util::GeometryType::GEOMETRY_TYPE_UNKNOWN) {
      include_empty_ = options.get_bool("include_empty", true);
    }

    void new_dimensions(util::Dimensions dim) {
        dim_ = dim;
    }

    void new_geometry_type(util::GeometryType geometry_type) {
        geometry_type_ = geometry_type;
    }

    Result geom_start(util::GeometryType geometry_type, int32_t size) {
        std::pair<util::GeometryType, util::Dimensions> item(geometry_type, dim_);
        if (!include_empty_ && size == 0) {
            empty_types_.insert(item);
        } else {
            all_types_.insert(item);
        }

        return Result::ABORT_FEATURE;
    }

    void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
        arrow::hpp::builder::StringArrayBuilder builder;

        if (all_types_.empty()) {
            for (const auto& item: empty_types_) {
                builder.write_element(make_type(item));
            }
        } else {
            for (const auto& item: all_types_) {
                builder.write_element(make_type(item));
            }
        }

        builder.release(array_data, schema);
    }

private:
    bool include_empty_;
    util::Dimensions dim_;
    util::GeometryType geometry_type_;
    std::unordered_set<std::pair<util::GeometryType, util::Dimensions>, type_dim_pair_hash> all_types_;
    std::unordered_set<std::pair<util::GeometryType, util::Dimensions>, type_dim_pair_hash> empty_types_;

    std::string make_type(std::pair<util::GeometryType, util::Dimensions> item) {
        const char* type_str = "";
        const char* dim_str = "";

        switch (item.first) {
        case util::GeometryType::POINT:
            type_str = "Point";
            break;
        case util::GeometryType::LINESTRING:
            type_str = "LineString";
            break;
        case util::GeometryType::POLYGON:
            type_str = "Polygon";
            break;
        case util::GeometryType::MULTIPOINT:
            type_str = "MultiPoint";
            break;
        case util::GeometryType::MULTILINESTRING:
            type_str = "MultiLineString";
            break;
        case util::GeometryType::MULTIPOLYGON:
            type_str = "MultiPolygon";
            break;
        case util::GeometryType::GEOMETRYCOLLECTION:
            type_str = "GeometryCollection";
            break;
        default:
            return "";
        }

        switch (item.second) {
        case util::Dimensions::XY:
            dim_str = "";
            break;
        case util::Dimensions::XYZ:
            dim_str = " Z";
            break;
        case util::Dimensions::XYM:
            dim_str = " M";
            break;
        case util::Dimensions::XYZM:
            dim_str = " ZM";
            break;
        default:
            // don't append anything with an unknown geometry type
            throw util::IOException("Can't append with unknown dimension type");
        }

        char out[128];
        memset(out, 0, sizeof(out));
        snprintf(out, sizeof(out), "%s%s", type_str, dim_str);
        return std::string(out);
    }
};

}
