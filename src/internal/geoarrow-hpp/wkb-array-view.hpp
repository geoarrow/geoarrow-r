
#pragma once

#include "wkb-reader.hpp"
#include "array-view.hpp"


namespace geoarrow {

namespace {

#ifndef bswap_32
static inline uint32_t bswap_32(uint32_t x) {
  return (((x & 0xFF) << 24) |
          ((x & 0xFF00) << 8) |
          ((x & 0xFF0000) >> 8) |
          ((x & 0xFF000000) >> 24));
}
#define bswap_32(x) bswap_32(x)
#endif

#ifndef bswap_64
static inline uint64_t bswap_64(uint64_t x) {
  return (((x & 0xFFULL) << 56) |
          ((x & 0xFF00ULL) << 40) |
          ((x & 0xFF0000ULL) << 24) |
          ((x & 0xFF000000ULL) << 8) |
          ((x & 0xFF00000000ULL) >> 8) |
          ((x & 0xFF0000000000ULL) >> 24) |
          ((x & 0xFF000000000000ULL) >> 40) |
          ((x & 0xFF00000000000000ULL) >> 56));
}
#define bswap_64(x) bswap_64(x)
#endif

}


class WKBArrayView: public ArrayView {
public:
    WKBArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

    void set_array(const struct ArrowArray* array) {
        ArrayView::set_array(array);
        offset_buffer_ = reinterpret_cast<const int32_t*>(array->buffers[1]);
        data_ = reinterpret_cast<const uint8_t*>(array->buffers[2]);
    }

    Handler::Result read_features(Handler* handler) {
        return internal::read_features_templ<WKBArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        int32_t start = offset_buffer_[array_->offset + offset];
        int32_t end = offset_buffer_[array_->offset + offset + 1];
        return reader_.read_buffer(handler, data_ + start, end - start);
    }

private:
    const int32_t* offset_buffer_;
    const uint8_t* data_;
    WKBReader reader_;
};

class LargeWKBArrayView: public ArrayView {
public:
    LargeWKBArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

    void set_array(const struct ArrowArray* array) {
        ArrayView::set_array(array);
        offset_buffer_ = reinterpret_cast<const int64_t*>(array->buffers[1]);
        data_ = reinterpret_cast<const uint8_t*>(array->buffers[2]);
    }

    Handler::Result read_features(Handler* handler) {
        return internal::read_features_templ<LargeWKBArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        int64_t start = offset_buffer_[array_->offset + offset];
        int64_t end = offset_buffer_[array_->offset + offset + 1];
        return reader_.read_buffer(handler, data_ + start, end - start);
    }

private:
    const int64_t* offset_buffer_;
    const uint8_t* data_;
    WKBReader reader_;
};

class FixedWidthWKBArrayView: public ArrayView {
public:
    FixedWidthWKBArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

    void set_array(const struct ArrowArray* array) {
        ArrayView::set_array(array);
        data_ = reinterpret_cast<const uint8_t*>(array->buffers[1]);
    }

    Handler::Result read_features(Handler* handler) {
        return internal::read_features_templ<FixedWidthWKBArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        int32_t start = meta_.fixed_width_ * (array_->offset + offset);
        return reader_.read_buffer(handler, data_ + start, meta_.fixed_width_);
    }

private:
    const uint8_t* data_;
    WKBReader reader_;
};

}
