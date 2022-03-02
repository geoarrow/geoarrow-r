
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


class WKBReader {
public:
    void read_feature(uint8_t* src, int64_t size) {

    }

private:
    bool swapping_;

};

class WKBArrayView: public ArrayView {
public:
    WKBArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

private:
    int32_t offsets_;
    uint8_t data_;
};

class LargeWKBArrayView: public ArrayView {
public:
    LargeWKBArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

};

class FixedWidthWKBArrayView: public ArrayView {
public:
    FixedWidthWKBArrayView(const struct ArrowSchema* schema): ArrayView(schema) {}

};



}
