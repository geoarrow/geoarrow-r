
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

ComputeBuilder* create_builder(compute::Operation op, const ComputeOptions& options) {
    if (op == compute::Operation::VOID) {
        return new NullBuilder();
    } else if (op == compute::Operation::CAST) {
        Meta geoarrow_meta(options.get_schema("schema"));

        switch (geoarrow_meta.extension_) {
        case util::Extension::WKT:
            return new WKTArrayBuilder();
        default:
            throw Meta::ValidationError("Unsupported extension type for operation CAST");
        }
    } else if (op == compute::Operation::GLOBAL_BOUNDS) {
        return new GlobalBounder();
    } else {
        throw util::IOException("Unsupported operation identifier");
    }
}

}
