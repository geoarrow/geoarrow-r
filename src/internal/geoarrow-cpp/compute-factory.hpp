
#pragma once

#include "common.hpp"
#include "meta.hpp"
#include "compute-builder.hpp"
#include "compute-cast-wkt.hpp"
#include "compute-cast-wkb.hpp"
#include "compute-cast-point.hpp"
#include "compute-cast-linestring.hpp"
#include "compute-cast-polygon.hpp"
#include "compute-cast-collection.hpp"
#include "compute-bounds.hpp"
#include "compute-geoparquet-types.hpp"

namespace geoarrow {

ComputeBuilder* create_builder(const std::string& op, const ComputeOptions& options);

#if defined(ARROW_HPP_IMPL)

ComputeBuilder* create_builder(const std::string& op, const ComputeOptions& options) {
    if (op == "void") {
        return new NullBuilder();
    } else if (op == "cast") {
        Meta geoarrow_meta(options.get_schema("schema"));

        switch (geoarrow_meta.extension_) {
        case util::Extension::WKT:
            return new WKTArrayBuilder(options);
        case util::Extension::WKB:
            return new WKBArrayBuilder(options);
        case util::Extension::Point:
            return new PointArrayBuilder(options);
        case util::Extension::Linestring:
            return new LinestringArrayBuilder(options);
        case util::Extension::Polygon:
            return new PolygonArrayBuilder(options);
        case util::Extension::MultiPoint:
            return new MultiPointArrayBuilder(options);
        case util::Extension::MultiLinestring:
            return new MultiLinestringArrayBuilder(options);
        case util::Extension::MultiPolygon:
            return new MultiPolygonArrayBuilder(options);
        default:
            throw Meta::ValidationError("Unsupported extension type for operation CAST");
        }
    } else if (op == "global_bounds") {
        return new GlobalBounder(options);
    } else if (op == "geoparquet_types") {
        return new GeoParquetTypeCollector(options);
    } else {
        throw util::IOException("Unknown operation: '%s'", op.c_str());
    }
}

#endif

}
