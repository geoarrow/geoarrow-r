
#pragma once

#include "port.h"
#ifdef IS_LITTLE_ENDIAN
#define GEOARROW_ENDIAN 0x01
#else
#define GEOARROW_ENDIAN 0x00
#endif

#include "internal/geoarrow-hpp/meta.hpp"
#include "internal/geoarrow-hpp/handler.hpp"
#include "internal/geoarrow-hpp/array-view-base.hpp"

namespace geoarrow {

ArrayView* create_view(struct ArrowSchema* schema);

}

#undef HANDLE_OR_RETURN
#undef HANDLE_CONTINUE_OR_BREAK
