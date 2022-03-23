
#pragma once

#include "common.hpp"
#include "meta.hpp"
#include "compute-builder.hpp"
#include "compute-cast-wkt.hpp"
#include "compute-bounds.hpp"

namespace geoarrow {

namespace compute {

enum Operation {
    VOID = 0,
    CAST = 1,
    GLOBAL_BOUNDS = 2,
    OP_INVALID = 3
};

}

ComputeBuilder* create_builder(compute::Operation op, struct ArrowSchema* schema, int64_t size) {
    Meta geoarrow_meta(schema);

    switch (op) {
    case compute::Operation::VOID:
        return new NullBuilder();

    case compute::Operation::CAST:
        switch (geoarrow_meta.extension_) {
        case util::Extension::WKT:
            return new WKTArrayBuilder(size, size);
        default:
            throw Meta::ValidationError("Unsupported extension type for operation CAST");
        }

    case compute::Operation::GLOBAL_BOUNDS:
        return new GlobalBounder();
    default:
        throw util::IOException("Unknown operation type");
    }
}

}
