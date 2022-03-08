
#pragma once

#include <cstdint>
#include "meta.hpp"

namespace geoarrow {

// A `Handler` is a stateful handler base class that responds to events
// as they are encountered while iterating over a `ArrayView`. This
// style of iteration is useful for certain types of operations, particularly
// if virtual method calls are a concern. You can also use `ArrayView`'s
// pull-style iterators to iterate over geometries.
class Handler {
public:
    enum Result {
        CONTINUE = 0,
        ABORT = 1,
        ABORT_FEATURE = 2,
        ABORT_ARRAY = 3
    };

    virtual void new_meta(const Meta* meta) {}
    virtual void new_dimensions(util::Dimensions geometry_type) {}

    virtual Result array_start(const struct ArrowArray* array_data) { return Result::CONTINUE; }
    virtual Result feat_start() { return Result::CONTINUE; }
    virtual Result null_feat() { return Result::CONTINUE; }
    virtual Result geom_start(util::GeometryType geometry_type, int32_t size) { return Result::CONTINUE; }
    virtual Result ring_start(int32_t size) { return Result::CONTINUE; }
    virtual Result coords(const double* coord, int64_t n, int32_t coord_size) { return Result::CONTINUE; }
    virtual Result ring_end() { return Result::CONTINUE; }
    virtual Result geom_end() { return Result::CONTINUE; }
    virtual Result feat_end() { return Result::CONTINUE; }
    virtual Result array_end() { return Result::CONTINUE; }

    virtual ~Handler() {}
};

}

#define HANDLE_OR_RETURN(expr)                                 \
    result = expr;                                             \
    if (result != Handler::Result::CONTINUE) return result

#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == Handler::Result::ABORT_FEATURE) \
        continue; \
    else if (result == Handler::Result::ABORT) break
