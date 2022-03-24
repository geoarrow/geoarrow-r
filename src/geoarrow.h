
#pragma once

#include "internal/geoarrow-hpp/handler.hpp"
#include "internal/geoarrow-hpp/array-view-base.hpp"
#include "internal/geoarrow-hpp/compute-builder.hpp"

namespace geoarrow {

ArrayView* create_view(struct ArrowSchema* schema);
ComputeBuilder* create_builder(const std::string& op, const ComputeOptions& options);

}

#undef HANDLE_OR_RETURN
#undef HANDLE_CONTINUE_OR_BREAK
