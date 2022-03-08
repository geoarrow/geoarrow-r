
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
        handler->new_schema(schema_);
        handler->new_geometry_type(meta_.geometry_type_);
        handler->new_dimensions(meta_.dimensions_);
    }

    virtual Handler::Result read_features(Handler* handler) {
        throw std::runtime_error("ArrayView::read_features() not implemented");
    }

    virtual void set_array(const struct ArrowArray* array) {
        if (!meta_.array_valid(array)) {
            throw Meta::ValidationError(meta_.error_);
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
    Meta meta_;
    int64_t feature_id_;
    const uint8_t* validity_buffer_;
};

namespace internal {

template <class TArrayView>
Handler::Result read_features_templ(TArrayView& view, Handler* handler) {
    Handler::Result result;

    result = handler->array_start(view.array_);
    if (result == Handler::Result::ABORT) {
        return result;
    }

    for (int64_t i = 0; i < view.array_->length; i++) {
        HANDLE_CONTINUE_OR_BREAK(view.read_feature(handler, i));
    }

    if (result == Handler::Result::ABORT) {
        return Handler::Result::ABORT;
    } else {
        return Handler::Result::CONTINUE;
    }

    return handler->array_end();
}

template <class TArrayView>
Handler::Result read_feature_templ(TArrayView& view, int64_t offset, Handler* handler) {
    Handler::Result result;
    HANDLE_OR_RETURN(handler->feat_start());

    if (view.is_null(offset)) {
        HANDLE_OR_RETURN(handler->null_feat());
    } else {
        HANDLE_OR_RETURN(view.read_geometry(handler, offset));
    }

    HANDLE_OR_RETURN(handler->feat_end());
    return Handler::Result::CONTINUE;
}

}

}
