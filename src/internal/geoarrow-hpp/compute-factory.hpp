
#pragma once

#include "common.hpp"
#include "meta.hpp"
#include "compute-builder.hpp"
#include "compute-cast-wkt.hpp"
#include "compute-bounds.hpp"

namespace geoarrow {

ComputeBuilder* create_builder(const std::string& op, const ComputeOptions& options) {
    if (op == "void") {
        return new NullBuilder();
    } else if (op == "cast") {
        Meta geoarrow_meta(options.get_schema("schema"));

        switch (geoarrow_meta.extension_) {
        case util::Extension::WKT:
            return new WKTArrayBuilder();
        default:
            throw Meta::ValidationError("Unsupported extension type for operation CAST");
        }
    } else if (op == "global_bounds") {
        return new GlobalBounder(options);
    } else {
        throw util::IOException("Unknown operation: '%s'", op.c_str());
    }
}

}
