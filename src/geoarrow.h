
#pragma once

#include "internal/geoarrow-hpp/handler.hpp"
#include "internal/geoarrow-hpp/array-view-base.hpp"
#include "internal/geoarrow-hpp/compute-builder.hpp"

namespace geoarrow {

namespace compute {

enum Operation {
    VOID = 0,
    CAST = 1,
    GLOBAL_BOUNDS = 2,
    OP_INVALID = 3
};

}

ArrayView* create_view(struct ArrowSchema* schema);
ComputeBuilder* create_builder(compute::Operation op, const ComputeOptions& options);

}

#undef HANDLE_OR_RETURN
#undef HANDLE_CONTINUE_OR_BREAK
