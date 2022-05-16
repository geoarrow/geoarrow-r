
#pragma once

#include <cmath>

#include "handler.hpp"

#define EWKB_Z_BIT 0x80000000
#define EWKB_M_BIT 0x40000000
#define EWKB_SRID_BIT 0x20000000

namespace geoarrow {

namespace {

#ifndef GEOARROW_ENDIAN
#define _GEOARROW_ENDIAN 0x01
#else
#define _GEOARROW_ENDIAN GEOARROW_ENDIAN
#endif

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
    WKBReader(): top_level_(true), data_(nullptr), offset_(0), size_(0), swapping_(false),
        dim_(util::Dimensions::DIMENSIONS_UNKNOWN),
        geometry_type_(util::GeometryType::GEOMETRY_TYPE_UNKNOWN) {
      for (int i = 0; i < 4; i++) {
        coord_[i] = NAN;
      }
    }

    Handler::Result read_buffer(Handler* handler, const uint8_t* data, int64_t size) {
      top_level_ = true;
      data_ = data;
      size_ = size;
      offset_ = 0;
      return read_geometry(handler);
    }

private:
    bool top_level_;
    const uint8_t* data_;
    int64_t offset_;
    int64_t size_;
    bool swapping_;
    util::Dimensions dim_;
    util::GeometryType geometry_type_;
    double coord_[4];

    Handler::Result read_geometry(Handler* handler) {
      read_endian();

      uint32_t geometry_type = read_uint32();
      bool has_z = false;
      bool has_m = false;

      if (geometry_type & EWKB_Z_BIT) {
        has_z = true;
      }

      if (geometry_type & EWKB_M_BIT) {
        has_m = true;
      }

      if (geometry_type & EWKB_SRID_BIT) {
        // we ignore this because it's hard to work around if a user somehow
        // has embedded srid but still wants the data and doesn't have another way
        // to convert
        read_uint32();
      }

      geometry_type = geometry_type & 0x0000ffff;

      if (geometry_type >= 3000) {
        geometry_type = geometry_type - 3000;
        has_z = true;
        has_m = true;
      } else  if (geometry_type >= 2000) {
        geometry_type = geometry_type - 2000;
        has_m = true;
      } else if (geometry_type >= 1000) {
        geometry_type = geometry_type - 1000;
        has_z = true;
      }

      uint32_t geometry_size;
      if (geometry_type == util::GeometryType::POINT) {
        geometry_size = 1;
      } else {
        geometry_size = read_uint32();
      }

      int32_t coord_size = 2 + has_z + has_m;
      auto geometry_type_enum = static_cast<util::GeometryType>(geometry_type);

      util::Dimensions new_dim;
      if (has_z && has_m) {
        new_dim = util::Dimensions::XYZM;
      } else if (has_z) {
        new_dim = util::Dimensions::XYZ;
      } else if (has_m) {
        new_dim = util::Dimensions::XYM;
      } else {
        new_dim = util::Dimensions::XY;
      }

      if (top_level_ && geometry_type_enum != geometry_type_) {
        handler->new_geometry_type(geometry_type_enum);
        geometry_type_ = geometry_type_enum;
      }

      if (new_dim != dim_) {
        handler->new_dimensions(new_dim);
        dim_ = new_dim;
      }

      top_level_ = false;

      Handler::Result result;

      HANDLE_OR_RETURN(handler->geom_start(geometry_type_enum, geometry_size));

      switch (geometry_type_enum) {
      case util::GeometryType::POINT:
      case util::GeometryType::LINESTRING:
        HANDLE_OR_RETURN(read_coords(handler, geometry_size, coord_size));
        break;
      case util::GeometryType::POLYGON:
        for (uint32_t i = 0; i < geometry_size; i++) {
          uint32_t n_coords = read_uint32();
          HANDLE_OR_RETURN(handler->ring_start(n_coords));
          HANDLE_OR_RETURN(read_coords(handler, n_coords, coord_size));
          HANDLE_OR_RETURN(handler->ring_end());
        }
        break;
      case util::GeometryType::MULTIPOINT:
      case util::GeometryType::MULTILINESTRING:
      case util::GeometryType::MULTIPOLYGON:
      case util::GeometryType::GEOMETRYCOLLECTION:
        for (uint32_t i = 0; i < geometry_size; i++) {
          HANDLE_OR_RETURN(read_geometry(handler));
        }
        break;
      default:
        throw util::IOException("Unrecognized geometry type: %d", geometry_type);
      }

      HANDLE_OR_RETURN(handler->geom_end());
      return Handler::Result::CONTINUE;
    }

    Handler::Result read_coords(Handler* handler, uint32_t n, int32_t coord_size) {
      check_buffer(sizeof(double) * coord_size * n);
      Handler::Result result;

      if (swapping_) {
        uint64_t tmp;
        for (uint32_t i = 0; i < n; i++) {
          for (int32_t j = 0; j < coord_size; j++) {
            tmp = bswap_64(read<uint64_t>());
            memcpy(coord_ + j, &tmp, sizeof(uint64_t));
          }

          HANDLE_OR_RETURN(handler->coords(coord_, 1, coord_size));
        }
      } else {
        for (uint32_t i = 0; i < n; i++) {
          memcpy(coord_, data_ + offset_, sizeof(double) * coord_size);
          offset_ += sizeof(double) * coord_size;
          HANDLE_OR_RETURN(handler->coords(coord_, 1, coord_size));
        }
      }

      return Handler::Result::CONTINUE;
    }

    void read_endian() {
      swapping_ = read_uint8() != _GEOARROW_ENDIAN;
    }

    template<typename T> T read() {
      T result;
      memcpy(&result, data_ + offset_, sizeof(T));
      offset_ += sizeof(T);
      return result;
    }

    uint32_t read_uint32() {
      check_buffer(sizeof(uint32_t));
      uint32_t result = read<uint32_t>();
      if (swapping_) {
        return(bswap_32(result));
      } else {
        return result;
      }
    }

    uint8_t read_uint8() {
      check_buffer(1);
      return read<uint8_t>();
    }

    void check_buffer(int64_t n) {
      if ((offset_ + n) > size_) {
        throw util::IOException(
            "Unexpected end of buffer at %lld + %lld / %lld",
             offset_, n, size_);
      }
    }
};

}

#undef _GEOARROW_ENDIAN
#undef EWKB_Z_BIT
#undef EWKB_M_BIT
#undef EWKB_SRID_BIT
