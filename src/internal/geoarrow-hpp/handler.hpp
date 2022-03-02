
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
        ABORT_FEATURE = 2
    };

    virtual void schema(const struct ArrowSchema* schema) {}
    virtual void new_geometry_type(Meta::GeometryType geometry_type) {}
    virtual void new_dimensions(Meta::Dimensions geometry_type) {}

    virtual Result array_start(const struct ArrowArray* array_data) { return Result::CONTINUE; }
    virtual Result feat_start() { return Result::CONTINUE; }
    virtual Result null_feat() { return Result::CONTINUE; }
    virtual Result geom_start(int32_t size) { return Result::CONTINUE; }
    virtual Result ring_start(int32_t size) { return Result::CONTINUE; }
    virtual Result coord(const double* coord) { return Result::CONTINUE; }
    virtual Result ring_end() { return Result::CONTINUE; }
    virtual Result geom_end() { return Result::CONTINUE; }
    virtual Result feat_end() { return Result::CONTINUE; }
    virtual Result array_end() { return Result::CONTINUE; }

    virtual ~Handler() {}
};

}
