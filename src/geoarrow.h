
#pragma once

#include "internal/geoarrow-hpp/handler.hpp"
#include "internal/geoarrow-hpp/array-view-base.hpp"

namespace geoarrow {

ArrayView* create_view(struct ArrowSchema* schema);

}

#undef HANDLE_OR_RETURN
#undef HANDLE_CONTINUE_OR_BREAK
