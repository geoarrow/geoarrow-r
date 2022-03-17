
#pragma once

#include "common.hpp"
#include "meta.hpp"
#include "array-builder-wkt.hpp"

namespace geoarrow {

namespace builder {

ArrayBuilder* create_builder(struct ArrowSchema* schema, int64_t size = 1024) {
    Meta geoarrow_meta(schema);

    switch (geoarrow_meta.extension_) {
    case util::Extension::WKT:
        return new WKTArrayBuilder(size, size);
    default:
        throw Meta::ValidationError("Unsupported extension type");
    }
}

}

}
