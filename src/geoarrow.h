
#pragma once

#include "internal/geoarrow-hpp/handler.hpp"
#include "internal/geoarrow-hpp/array-view-base.hpp"
#include "internal/geoarrow-hpp/array-builder.hpp"

namespace geoarrow {

ArrayView* create_view(struct ArrowSchema* schema);
GeoArrayBuilder* create_builder(struct ArrowSchema* schema, int64_t size);

}

#undef HANDLE_OR_RETURN
#undef HANDLE_CONTINUE_OR_BREAK
