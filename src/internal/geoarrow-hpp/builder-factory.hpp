
#pragma once

#include "common.hpp"
#include "meta.hpp"
#include "array-builder-wkt.hpp"

namespace geoarrow {

GeoArrayBuilder* create_builder(struct ArrowSchema* schema, int64_t size) {
    Meta geoarrow_meta(schema);

    // Special-case the null builder, which is used for profiling overhead
    if (geoarrow_meta.storage_type_ == util::StorageType::Null) {
        return new NullBuilder();
    }

    switch (geoarrow_meta.extension_) {
    case util::Extension::WKT:
        return new WKTArrayBuilder(size, size);
    default:
        throw Meta::ValidationError("Unsupported extension type");
    }
}

}
