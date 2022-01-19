
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "wk-v1.h"
#include "narrow.h"
#include "util.h"

#define FASTFLOAT_ASSERT(x) { if (!(x)) Rf_error("fastfloat assert failed"); }
#include "internal/buffered-reader.hpp"

#define HANDLE_OR_RETURN(expr)                                 \
  result = expr;                                               \
  if (result != WK_CONTINUE) return result

#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
  result = expr;                                               \
  if (result == WK_ABORT_FEATURE) continue; else if (result == WK_ABORT) break


// The BufferedWKTParser is the BufferedParser subclass with methods specific
// to well-known text. It doesn't know about any particular output format.
template <class SourceType>
class BufferedWKTParser: public BufferedParser<SourceType, 4096> {
public:

  BufferedWKTParser() {
    this->setSeparators(" \r\n\t,();=");
  }

  void assertGeometryMeta(wk_meta_t* meta) {
    std::string geometry_type = this->assertWord();

    if (geometry_type == "SRID") {
      this->assert_('=');
      meta->srid = this->assertInteger();
      this->assert_(';');
      geometry_type = this->assertWord();
    }

    meta->geometry_type = this->geometry_typeFromString(geometry_type);

    if (this->is('Z')) {
      this->assert_('Z');
      meta->flags |= WK_FLAG_HAS_Z;
    }

    if (this->is('M')) {
      this->assert_('M');
      meta->flags |= WK_FLAG_HAS_M;
    }

    if (this->isEMPTY()) {
      meta->size = 0;
    }
  }

  int geometry_typeFromString(std::string geometry_type) {
    if (geometry_type == "POINT") {
      return WK_POINT;
    } else if(geometry_type == "LINESTRING") {
      return WK_LINESTRING;
    } else if(geometry_type == "POLYGON") {
      return WK_POLYGON;
    } else if(geometry_type == "MULTIPOINT") {
      return WK_MULTIPOINT;
    } else if(geometry_type == "MULTILINESTRING") {
      return WK_MULTILINESTRING;
    } else if(geometry_type == "MULTIPOLYGON") {
      return WK_MULTIPOLYGON;
    } else if(geometry_type == "GEOMETRYCOLLECTION") {
      return WK_GEOMETRYCOLLECTION;
    } else {
      this->errorBefore("geometry type or 'SRID='", geometry_type);
    }
  }

  bool isEMPTY() {
    return this->peekUntilSep() == "EMPTY";
  }

  bool assertEMPTYOrOpen() {
    if (this->isLetter()) {
      std::string word = this->assertWord();
      if (word != "EMPTY") {
        this->errorBefore("'(' or 'EMPTY'", word);
      }

      return true;
    } else if (this->is('(')) {
      this->assert_('(');
      return false;
    } else {
      this->error("'(' or 'EMPTY'");
    }
  }
};


// The BufferedWKTReader knows about wk_handler_t and does all the "driving". The
// entry point is readFeature(), which does not throw (but may longjmp).
// The BufferedWKTReader is carefully designed to (1) avoid any virtual method calls
// (via templating) and (2) to avoid using any C++ objects with non-trivial destructors.
// The non-trivial destructors bit is important because handler methods can and do longjmp
// when used in R. The object itself does not have a non-trivial destructor and it's expected
// that the scope in which it is declared uses the proper unwind-protection such that the
// object and its members are deleted.
template <class SourceType, typename handler_t>
class BufferedWKTReader {
public:

  BufferedWKTReader(handler_t* handler): handler(handler) {
    memset(this->error_message, 0, sizeof(this->error_message));
  }

  int readFeature(wk_vector_meta_t* meta, int64_t feat_id, SourceType* source) {
    try {
      int result;
      HANDLE_OR_RETURN(this->handler->feature_start(meta, feat_id, this->handler->handler_data));

      if (source == nullptr) {
        HANDLE_OR_RETURN(this->handler->null_feature(this->handler->handler_data));
      } else {
        s.setSource(source);
        HANDLE_OR_RETURN(this->readGeometryWithType(WK_PART_ID_NONE));
        s.assertFinished();
      }

      return this->handler->feature_end(meta, feat_id, this->handler->handler_data);
    } catch (std::exception& e) {
      // can't call a handler method that longjmps here because `e` must be deleted
      memset(this->error_message, 0, sizeof(this->error_message));
      strncpy(this->error_message, e.what(), sizeof(this->error_message) - 1);
    }

    return this->handler->error(this->error_message, this->handler->handler_data);
  }

protected:

  int readGeometryWithType(uint32_t part_id) {
    wk_meta_t meta;
    WK_META_RESET(meta, WK_GEOMETRY);
    s.assertGeometryMeta(&meta);

    int result;
    HANDLE_OR_RETURN(this->handler->geometry_start(&meta, part_id, this->handler->handler_data));

    switch (meta.geometry_type) {

    case WK_POINT:
      HANDLE_OR_RETURN(this->readPoint(&meta));
      break;

    case WK_LINESTRING:
      HANDLE_OR_RETURN(this->readLineString(&meta));
      break;

    case WK_POLYGON:
      HANDLE_OR_RETURN(this->readPolygon(&meta));
      break;

    case WK_MULTIPOINT:
      HANDLE_OR_RETURN(this->readMultiPoint(&meta));
      break;

    case WK_MULTILINESTRING:
      HANDLE_OR_RETURN(this->readMultiLineString(&meta));
      break;

    case WK_MULTIPOLYGON:
      HANDLE_OR_RETURN(this->readMultiPolygon(&meta));
      break;

    case WK_GEOMETRYCOLLECTION:
      HANDLE_OR_RETURN(this->readGeometryCollection(&meta));
      break;

    default:
      throw std::runtime_error("Unknown geometry type"); // # nocov
    }

    return this->handler->geometry_end(&meta, part_id, this->handler->handler_data);
  }

  int readPoint(const wk_meta_t* meta) {
    if (!s.assertEMPTYOrOpen()) {
      int result;
      HANDLE_OR_RETURN(this->readPointCoordinate(meta));
      s.assert_(')');
    }

    return WK_CONTINUE;
  }

  int readLineString(const wk_meta_t* meta) {
    return this->readCoordinates(meta);
  }

  int readPolygon(const wk_meta_t* meta)  {
    return this->readLinearRings(meta);
  }

  int readMultiPoint(const wk_meta_t* meta) {
    if (s.assertEMPTYOrOpen()) {
      return WK_CONTINUE;
    }

    wk_meta_t childMeta;
    WK_META_RESET(childMeta, WK_POINT);
    uint32_t part_id = 0;
    int result;

    if (s.isNumber()) { // (0 0, 1 1)
      do {
        this->readChildMeta(meta, &childMeta);

        HANDLE_OR_RETURN(this->handler->geometry_start(&childMeta, part_id, this->handler->handler_data));

        if (s.isEMPTY()) {
          s.assertWord();
        } else {
          HANDLE_OR_RETURN(this->readPointCoordinate(&childMeta));
        }
        HANDLE_OR_RETURN(this->handler->geometry_end(&childMeta, part_id, this->handler->handler_data));

        part_id++;
      } while (s.assertOneOf(",)") != ')');

    } else { // ((0 0), (1 1))
      do {
        this->readChildMeta(meta, &childMeta);
        HANDLE_OR_RETURN(this->handler->geometry_start(&childMeta, part_id, this->handler->handler_data));
        HANDLE_OR_RETURN(this->readPoint(&childMeta));
        HANDLE_OR_RETURN(this->handler->geometry_end(&childMeta, part_id, this->handler->handler_data));
        part_id++;
      } while (s.assertOneOf(",)") != ')');
    }

    return WK_CONTINUE;
  }

  int readMultiLineString(const wk_meta_t* meta) {
    if (s.assertEMPTYOrOpen()) {
      return WK_CONTINUE;
    }

    wk_meta_t childMeta;
    WK_META_RESET(childMeta, WK_LINESTRING);
    uint32_t part_id = 0;
    int result;

    do {
      this->readChildMeta(meta, &childMeta);
      HANDLE_OR_RETURN(this->handler->geometry_start(&childMeta, part_id, this->handler->handler_data));
      HANDLE_OR_RETURN(this->readLineString(&childMeta));
      HANDLE_OR_RETURN(this->handler->geometry_end(&childMeta, part_id, this->handler->handler_data));

      part_id++;
    } while (s.assertOneOf(",)") != ')');

    return WK_CONTINUE;
  }

  uint32_t readMultiPolygon(const wk_meta_t* meta) {
    if (s.assertEMPTYOrOpen()) {
      return WK_CONTINUE;
    }

    wk_meta_t childMeta;
    WK_META_RESET(childMeta, WK_POLYGON);
    uint32_t part_id = 0;
    int result;

    do {
      this->readChildMeta(meta, &childMeta);
      HANDLE_OR_RETURN(this->handler->geometry_start(&childMeta, part_id, this->handler->handler_data));
      HANDLE_OR_RETURN(this->readPolygon(&childMeta));
      HANDLE_OR_RETURN(this->handler->geometry_end(&childMeta, part_id, this->handler->handler_data));
      part_id++;
    } while (s.assertOneOf(",)") != ')');

    return WK_CONTINUE;
  }

  int readGeometryCollection(const wk_meta_t* meta) {
    if (s.assertEMPTYOrOpen()) {
      return WK_CONTINUE;
    }

    uint32_t part_id = 0;
    int result;

    do {
      HANDLE_OR_RETURN(this->readGeometryWithType(part_id));
      part_id++;
    } while (s.assertOneOf(",)") != ')');

    return WK_CONTINUE;
  }

  uint32_t readLinearRings(const wk_meta_t* meta) {
    if (s.assertEMPTYOrOpen()) {
      return WK_CONTINUE;
    }

    uint32_t ring_id = 0;
    int result;

    do {
      HANDLE_OR_RETURN(this->handler->ring_start(meta, WK_SIZE_UNKNOWN, ring_id, this->handler->handler_data));
      HANDLE_OR_RETURN(this->readCoordinates(meta));
      HANDLE_OR_RETURN(this->handler->ring_end(meta, WK_SIZE_UNKNOWN, ring_id, this->handler->handler_data));
      ring_id++;
    } while (s.assertOneOf(",)") != ')');

    return WK_CONTINUE;
  }

  // Point coordinates are special in that there can only be one
  // coordinate (and reading more than one might cause errors since
  // writers are unlikely to expect a point geometry with many coordinates).
  // This assumes that `s` has already been checked for EMPTY or an opener
  // since this is different for POINT (...) and MULTIPOINT (.., ...)
  int readPointCoordinate(const wk_meta_t* meta) {
    double coord[4];
    int result;
    int coordSize = 2;
    if (meta->flags & WK_FLAG_HAS_Z) coordSize++;
    if (meta->flags & WK_FLAG_HAS_M) coordSize++;

    this->readCoordinate(coord, coordSize);
    HANDLE_OR_RETURN(handler->coord(meta, coord, 0, this->handler->handler_data));
    return WK_CONTINUE;
  }

  int readCoordinates(const wk_meta_t* meta) {
    double coord[4];
    int coordSize = 2;
    if (meta->flags & WK_FLAG_HAS_Z) coordSize++;
    if (meta->flags & WK_FLAG_HAS_M) coordSize++;

    if (s.assertEMPTYOrOpen()) {
      return WK_CONTINUE;
    }

    uint32_t coord_id = 0;
    int result;

    do {
      this->readCoordinate(coord, coordSize);
      HANDLE_OR_RETURN(handler->coord(meta, coord, coord_id, this->handler->handler_data));

      coord_id++;
    } while (s.assertOneOf(",)") != ')');

    return WK_CONTINUE;
  }

  void readCoordinate(double* coord, int coordSize) {
    coord[0] = s.assertNumber();
    for (int i = 1; i < coordSize; i++) {
      s.assertWhitespace();
      coord[i] = s.assertNumber();
    }
  }

  void readChildMeta(const wk_meta_t* parent, wk_meta_t* childMeta) {
    childMeta->flags = parent->flags;
    childMeta->srid = parent->srid;

    if (s.isEMPTY()) {
      childMeta->size = 0;
    } else {
      childMeta->size = WK_SIZE_UNKNOWN;
    }
  }

private:
  handler_t* handler;
  BufferedWKTParser<SourceType> s;
  char error_message[8096];
};


template <class T>
void finalize_cpp_xptr(SEXP xptr) {
  T* ptr = (T*) R_ExternalPtrAddr(xptr);
  if (ptr != nullptr) {
    delete ptr;
  }
}

SEXP geoarrow_read_wkt(SEXP data, wk_handler_t* handler) {
  struct ArrowArrayStream* array_stream = array_stream_from_xptr(VECTOR_ELT(data, 0), "handleable");
  struct ArrowSchema* schema = schema_from_xptr(VECTOR_ELT(data, 1), "schema");
  SEXP n_features_sexp = VECTOR_ELT(data, 2);

  int32_t* offset_buffer = NULL;
  int64_t* large_offset_buffer = NULL;
  char* data_buffer = NULL;

  int64_t start_offset, end_offset;
  int64_t fixed_width = -1;

  wk_vector_meta_t vector_meta;
  WK_VECTOR_META_RESET(vector_meta, WK_GEOMETRY);
  vector_meta.flags |= WK_FLAG_DIMS_UNKNOWN;

  if (TYPEOF(n_features_sexp) == INTSXP) {
      if (INTEGER(n_features_sexp)[0] != NA_INTEGER) {
          vector_meta.size = INTEGER(n_features_sexp)[0];
      }
  } else {
      double n_features_double = REAL(n_features_sexp)[0];
      if (!ISNA(n_features_double) && !ISNAN(n_features_double)) {
          vector_meta.size = n_features_double;
      }
  }

  // These are C++ objects but they are trivially destructible
  // (so longjmp in this frame is OK).
  SimpleBufferSource source;
  BufferedWKTReader<SimpleBufferSource, wk_handler_t> reader(handler);

  int result = handler->vector_start(&vector_meta, handler->handler_data);
  if (result != WK_ABORT) {

    struct ArrowArray* array_data = (struct ArrowArray*) malloc(sizeof(struct ArrowArray));
    if (array_data == NULL) {
        Rf_error("Failed to allocate struct ArrowArray");
    }
    array_data->release = NULL;
    SEXP array_data_wrapper = PROTECT(R_MakeExternalPtr(array_data, R_NilValue, R_NilValue));
    R_RegisterCFinalizer(array_data_wrapper, &geoarrow_finalize_array_data);

    int stream_result = 0;
    int64_t feat_id = -1;
    while(result != WK_ABORT) {
      if (array_data->release != NULL) {
        array_data->release(array_data);
      }
      stream_result = array_stream->get_next(array_stream, array_data);
      if (stream_result != 0) {
          const char* error_message = array_stream->get_last_error(array_stream);
          if (error_message != NULL) {
              Rf_error("[%d] %s", stream_result, error_message);
          } else {
              Rf_error("ArrowArrayStream->get_next() failed with code %d", stream_result);
          }
      }

      if (array_data->release == NULL) {
          break;
      }

      switch (schema->format[0]) {
      case 'z':
      case 'u':
        offset_buffer = (int32_t*) array_data->buffers[1];
        data_buffer = (char*) array_data->buffers[2];
        break;
      case 'Z':
      case 'U':
        large_offset_buffer = (int64_t*) array_data->buffers[1];
        data_buffer = (char*) array_data->buffers[2];
        break;
      case 'w':
        if (schema->format[1] == ':') {
          data_buffer =(char*) array_data->buffers[1];
          fixed_width = atol(schema->format + 2);
          if (fixed_width <= 0) {
            Rf_error("width must be >= 0");
          }
          break;
        }

        default:
            Rf_error("Can't handle schema format '%s' as WKT", schema->format);
        }

      unsigned char* validity_buffer = (unsigned char*) array_data->buffers[0];

      for (R_xlen_t i = 0; i < array_data->length; i++) {
        if (((i + 1) % 1000) == 0) R_CheckUserInterrupt();
        feat_id++;

        if (fixed_width > 0) {
          start_offset = i * fixed_width;
          end_offset = start_offset + fixed_width;
        } else if (offset_buffer != NULL) {
          start_offset = offset_buffer[i];
          end_offset = offset_buffer[i + 1];
        } else if (large_offset_buffer != NULL) {
          start_offset = large_offset_buffer[i];
          end_offset = large_offset_buffer[i + 1];
        } else {
          Rf_error("Can't locate feature in buffers"); // # nocov
        }

        if (validity_buffer && (0 == (validity_buffer[i / 8] & (1 << (i % 8))))) {
          HANDLE_CONTINUE_OR_BREAK(reader.readFeature(&vector_meta, feat_id, nullptr));
        } else {
          source.set_buffer(data_buffer + start_offset, end_offset - start_offset);
          HANDLE_CONTINUE_OR_BREAK(reader.readFeature(&vector_meta, feat_id, &source));
        }

        if (result == WK_ABORT) {
          break;
        }
      }
    }

    UNPROTECT(1);
  }

  return handler->vector_end(&vector_meta, handler->handler_data);
}

extern "C" SEXP geoarrow_c_handle_wkt(SEXP data, SEXP handler_xptr) {
  return wk_handler_run_xptr(&geoarrow_read_wkt, data, handler_xptr);
}
