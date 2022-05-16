
#pragma once

#include "wkt-reader.hpp"
#include "array-view-base.hpp"

namespace geoarrow {

class WKTArrayView: public ArrayView {
public:
    WKTArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

    void set_array(const struct ArrowArray* array) {
        ArrayView::set_array(array);
        offset_buffer_ = reinterpret_cast<const int32_t*>(array->buffers[1]);
        data_ = reinterpret_cast<const uint8_t*>(array->buffers[2]);
    }

    Handler::Result read_features(Handler* handler) {
        return internal::read_features_templ<WKTArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return internal::read_feature_templ<WKTArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        int32_t start = offset_buffer_[array_->offset + offset];
        int32_t end = offset_buffer_[array_->offset + offset + 1];
        return reader_.read_buffer(handler, data_ + start, end - start);
    }

private:
    const int32_t* offset_buffer_;
    const uint8_t* data_;
    WKTReader reader_;
};

class LargeWKTArrayView: public ArrayView {
public:
    LargeWKTArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

    void set_array(const struct ArrowArray* array) {
        ArrayView::set_array(array);
        offset_buffer_ = reinterpret_cast<const int64_t*>(array->buffers[1]);
        data_ = reinterpret_cast<const uint8_t*>(array->buffers[2]);
    }

    Handler::Result read_features(Handler* handler) {
        return internal::read_features_templ<LargeWKTArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return internal::read_feature_templ<LargeWKTArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        int64_t start = offset_buffer_[array_->offset + offset];
        int64_t end = offset_buffer_[array_->offset + offset + 1];
        return reader_.read_buffer(handler, data_ + start, end - start);
    }

private:
    const int64_t* offset_buffer_;
    const uint8_t* data_;
    WKTReader reader_;
};

}
