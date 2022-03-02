
#pragma once

#include "meta.hpp"
#include "handler.hpp"

namespace geoarrow {

// The `ArrayView` is the main class that uses of the API will interact with.
// it is an abstract class that represents a view of a `struct ArrowSchema` and
// a sequence of `struct ArrowArray`s, both of which must be valid pointers for the
// lifetime of the `ArrayView`. The `ArrayView` supports handler-style
// iteration using a `Handler` and pull-style iteration using virtual methods.
// The handler-style iteration is particularly useful as a way to write general-purpose
// code without using virtual methods.
class ArrayView {
  public:
    ArrayView(const struct ArrowSchema* schema):
      schema_(schema), array_(nullptr), meta_(schema),
      feature_id_(-1), validity_buffer_(nullptr) {}

    virtual ~ArrayView() {}

    void read_meta(Handler* handler) {
        handler->schema(schema_);
        handler->new_geometry_type(meta_.geometry_type_);
        handler->new_dimensions(meta_.dimensions_);
    }

    virtual Handler::Result read_features(Handler* handler) {
        throw std::runtime_error("ArrayView::read_features() not implemented");
    }

    virtual void set_array(const struct ArrowArray* array) {
        if (!meta_.array_valid(array)) {
            throw GeoArrowMeta::ValidationError(meta_.error_);
        }

        array_ = array;
        validity_buffer_ = reinterpret_cast<const uint8_t*>(array->buffers[0]);
    }

    bool is_null(int64_t offset) {
        return validity_buffer_ &&
            (validity_buffer_[offset / 8] & (0x01 << (offset % 8))) == 0;
    }

    const struct ArrowSchema* schema_;
    const struct ArrowArray* array_;
    GeoArrowMeta meta_;
    int64_t feature_id_;
    const uint8_t* validity_buffer_;
};

}
