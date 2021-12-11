#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "wk-v1.h"
#include "carrow.h"
#include <memory.h>
#include <stdint.h>
#include <stdarg.h>

#define WK_DEFAULT_ERROR_CODE 0
#define WK_NO_ERROR_CODE -1

// IS_BIG_ENDIAN, IS_LITTLE_ENDIAN, bswap_32(), bswap_64()
#include "port.h"

#define EWKB_Z_BIT 0x80000000
#define EWKB_M_BIT 0x40000000
#define EWKB_SRID_BIT 0x20000000

typedef struct {
    wk_handler_t* handler;
    int64_t feat_id;
    unsigned char* buffer;
    size_t size;
    size_t offset;
    char swap_endian;
    int error_code;
    char error_buf[1024];
} wkb_reader_t;

int wkb_read_geometry(wkb_reader_t* reader, uint32_t part_id);
int wkb_read_endian(wkb_reader_t* reader, unsigned char* value);
int wkb_read_geometry_type(wkb_reader_t* reader, wk_meta_t* meta);
int wkb_read_uint(wkb_reader_t* reader, uint32_t* value);
int wkb_read_coordinates(wkb_reader_t* reader, const wk_meta_t* meta, uint32_t n_coords, int n_dim);
void wkb_read_set_errorf(wkb_reader_t* reader, const char* error_buf, ...);

static inline int wkb_read_check_buffer(wkb_reader_t* reader, int64_t bytes) {
  int64_t bytes_to_keep = reader->size - reader->offset;
  if ((bytes_to_keep - bytes) >= 0) {
      return WK_CONTINUE;
  } else {
      wkb_read_set_errorf(reader, "Unexpected end of buffer at %d bytes", reader->offset);
      return WK_ABORT_FEATURE;
  }
}

#define HANDLE_OR_RETURN(expr)                                 \
    result = expr;                                             \
    if (result != WK_CONTINUE) return result

#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == WK_ABORT_FEATURE) continue; else if (result == WK_ABORT) break

int wkb_read_geometry(wkb_reader_t* reader, uint32_t part_id) {
    int result;

    unsigned char endian;
    HANDLE_OR_RETURN(wkb_read_endian(reader, &endian));

#ifdef IS_LITTLE_ENDIAN
    reader->swap_endian = endian != 1;
#else
    reader->swap_endian = endian != 0;
#endif

    wk_meta_t meta;
    WK_META_RESET(meta, WK_GEOMETRY);
    HANDLE_OR_RETURN(wkb_read_geometry_type(reader, &meta));
    int n_dim = 2 + ((meta.flags & WK_FLAG_HAS_Z) != 0) + ((meta.flags & WK_FLAG_HAS_M) != 0);

    HANDLE_OR_RETURN(reader->handler->geometry_start(&meta, part_id, reader->handler->handler_data));

    switch (meta.geometry_type) {
    case WK_POINT:
    case WK_LINESTRING:
        HANDLE_OR_RETURN(wkb_read_coordinates(reader, &meta, meta.size, n_dim));
        break;
    case WK_POLYGON:
        for (uint32_t i = 0; i < meta.size; i++) {
            uint32_t n_coords;
            HANDLE_OR_RETURN(wkb_read_uint(reader, &n_coords));
            HANDLE_OR_RETURN(reader->handler->ring_start(&meta, n_coords, i, reader->handler->handler_data));
            HANDLE_OR_RETURN(wkb_read_coordinates(reader, &meta, n_coords, n_dim));
            HANDLE_OR_RETURN(reader->handler->ring_end(&meta, n_coords, i, reader->handler->handler_data));
        }
        break;
    case WK_MULTIPOINT:
    case WK_MULTILINESTRING:
    case WK_MULTIPOLYGON:
    case WK_GEOMETRYCOLLECTION:
        for (uint32_t i = 0; i < meta.size; i++) {
            HANDLE_OR_RETURN(wkb_read_geometry(reader, i));
        }
        break;
    default:
        wkb_read_set_errorf(reader, "Unrecognized geometry type code '%d'", meta.geometry_type);
        return WK_ABORT_FEATURE;
    }

    return reader->handler->geometry_end(&meta, part_id, reader->handler->handler_data);
}

int wkb_read_endian(wkb_reader_t* reader, unsigned char* value) {
    int result;
    HANDLE_OR_RETURN(wkb_read_check_buffer(reader, sizeof(unsigned char)));
    memcpy(value, reader->buffer + reader->offset, sizeof(unsigned char));
    reader->offset += sizeof(unsigned char);
    return WK_CONTINUE;
}

int wkb_read_uint(wkb_reader_t* reader, uint32_t* value) {
    int result;
    HANDLE_OR_RETURN(wkb_read_check_buffer(reader, sizeof(uint32_t)));
    if (reader->swap_endian) {
        uint32_t swappable;
        memcpy(&swappable, reader->buffer + reader->offset, sizeof(uint32_t));
        reader->offset += sizeof(uint32_t);
        *value = bswap_32(swappable);
    } else {
        memcpy(value, reader->buffer + reader->offset, sizeof(uint32_t));
        reader->offset += sizeof(uint32_t);
    }

    return WK_CONTINUE;
}

int wkb_read_geometry_type(wkb_reader_t* reader, wk_meta_t* meta) {
    int result;
    uint32_t geometry_type;
    HANDLE_OR_RETURN(wkb_read_uint(reader, &geometry_type));

    if (geometry_type & EWKB_Z_BIT) {
        meta->flags |= WK_FLAG_HAS_Z;
    }

    if (geometry_type & EWKB_M_BIT) {
      meta->flags |= WK_FLAG_HAS_M;
    }

    if (geometry_type & EWKB_SRID_BIT) {
        HANDLE_OR_RETURN(wkb_read_uint(reader, &(meta->srid)));
    }

    geometry_type = geometry_type & 0x0000ffff;

    if (geometry_type >= 3000) {
        meta->geometry_type = geometry_type - 3000;
        meta->flags |= WK_FLAG_HAS_Z;
        meta->flags |= WK_FLAG_HAS_M;
    } else  if (geometry_type >= 2000) {
        meta->geometry_type = geometry_type - 2000;
        meta->flags |= WK_FLAG_HAS_M;
    } else if (geometry_type >= 1000) {
        meta->geometry_type = geometry_type - 1000;
        meta->flags |= WK_FLAG_HAS_Z;
    } else {
        meta->geometry_type = geometry_type;
    }

    if (meta->geometry_type == WK_POINT) {
        meta->size = 1;
    } else {
        HANDLE_OR_RETURN(wkb_read_uint(reader, &(meta->size)));
    }

    return WK_CONTINUE;
}

int wkb_read_coordinates(wkb_reader_t* reader, const wk_meta_t* meta, uint32_t n_coords, int n_dim) {
    double coord[4];
    int result;
    HANDLE_OR_RETURN(wkb_read_check_buffer(reader, sizeof(uint64_t) * n_dim * n_coords));

    if (reader->swap_endian) {
        uint64_t swappable, swapped;
        for (uint32_t i = 0; i < n_coords; i++) {
            

            for (int j = 0; j < n_dim; j++) {
                memcpy(&swappable, reader->buffer + reader->offset, sizeof(uint64_t));
                reader->offset += sizeof(double);

                swapped = bswap_64(swappable);
                memcpy(coord + j, &swapped, sizeof(double));
            }

            HANDLE_OR_RETURN(reader->handler->coord(meta, coord, i, reader->handler->handler_data));
        }
    } else {
        // seems to be slightly faster than memcpy(coord, ..., coord_size)
        uint64_t swappable;
        for (uint32_t i = 0; i < n_coords; i++) {
            for (int j = 0; j < n_dim; j++) {
                memcpy(&swappable, reader->buffer + reader->offset, sizeof(uint64_t));
                reader->offset += sizeof(double);
                memcpy(coord + j, &swappable, sizeof(double));
            }

            HANDLE_OR_RETURN(reader->handler->coord(meta, coord, i, reader->handler->handler_data));
        }
    }

    return WK_CONTINUE;
}

void wkb_read_set_errorf(wkb_reader_t* reader, const char* error_buf, ...) {
    reader->error_code = WK_DEFAULT_ERROR_CODE;
    va_list args;
    va_start(args, error_buf);
    vsnprintf(reader->error_buf, 1024, error_buf, args);
    va_end(args);
}

SEXP geoarrow_read_wkb(SEXP data, wk_handler_t* handler) {
    struct ArrowSchema* schema = schema_from_xptr(VECTOR_ELT(data, 0), "handleable$schema");
    struct ArrowArray* array_data = array_data_from_xptr(VECTOR_ELT(data, 1), "handleable$array_data");

    int32_t* offset_buffer = NULL;
    int64_t* large_offset_buffer = NULL;
    unsigned char* data_buffer = NULL;

    int64_t n_features = array_data->length;
    int64_t start_offset, end_offset;
    int64_t fixed_width = -1;

    switch (schema->format[0]) {
    case 'z':
        if (array_data->n_buffers != 3) {
            Rf_error("Expected ArrowArray with 3 buffers but got %d", array_data->n_buffers);
        }
        offset_buffer = (int32_t*) array_data->buffers[1];
        data_buffer = (unsigned char*) array_data->buffers[2];
        break;
    case 'Z':
        if (array_data->n_buffers != 3) {
            Rf_error("Expected ArrowArray with 3 buffers but got %d", array_data->n_buffers);
        }
        large_offset_buffer = (int64_t*) array_data->buffers[1];
        data_buffer = (unsigned char*) array_data->buffers[2];
        break;
    case 'w':
        if (array_data->n_buffers != 2) {
            Rf_error("Expected ArrowArray with 2 buffers but got %d", array_data->n_buffers);
        }
        if (schema->format[1] == ':') {
            data_buffer =(unsigned char*) array_data->buffers[1];
            fixed_width = atol(schema->format + 2);
            if (fixed_width <= 0) {
                Rf_error("width must be >= 0");
            }
            break;
        }

    default:
        Rf_error("Can't handle schema format '%s' as WKB", schema->format);
    }

    unsigned char* validity_buffer = (unsigned char*) array_data->buffers[0];

    wk_vector_meta_t vector_meta;
    WK_VECTOR_META_RESET(vector_meta, WK_GEOMETRY);
    vector_meta.size = n_features;
    vector_meta.flags |= WK_FLAG_DIMS_UNKNOWN;

    if (handler->vector_start(&vector_meta, handler->handler_data) == WK_CONTINUE) {
        int result;
        wkb_reader_t reader;
        reader.handler = handler;
        memset(reader.error_buf, 0, 1024);

        for (int64_t i = array_data->offset; i < n_features; i++) {
            if (((i + 1) % 1000) == 0) R_CheckUserInterrupt();

            reader.feat_id = i;
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

            HANDLE_CONTINUE_OR_BREAK(handler->feature_start(&vector_meta, i, handler->handler_data));

            if (validity_buffer && (0 == (validity_buffer[i / 8] & (1 << (i % 8))))) {
                HANDLE_CONTINUE_OR_BREAK(handler->null_feature(handler->handler_data));
            } else {
                reader.offset = 0;
                reader.size = end_offset - start_offset;
                reader.buffer = data_buffer + start_offset;
                reader.error_code = WK_NO_ERROR_CODE;
                reader.error_buf[0] = '\0';

                result = wkb_read_geometry(&reader, WK_PART_ID_NONE);
                if (result == WK_ABORT_FEATURE && reader.error_code != WK_NO_ERROR_CODE) {
                    result = handler->error(reader.error_buf, handler->handler_data);
                }

                if (result == WK_ABORT_FEATURE) {
                    continue;
                } else if (result == WK_ABORT) {
                    break;
                }
            }

            if (handler->feature_end(&vector_meta, i, handler->handler_data) == WK_ABORT) {
                break; // # nocov
            }
        }
    }

    SEXP result = PROTECT(handler->vector_end(&vector_meta, handler->handler_data));
    UNPROTECT(1);
    return result;
}

SEXP geoarrow_c_handle_wkb(SEXP data, SEXP handler_xptr) {
    return wk_handler_run_xptr(&geoarrow_read_wkb, data, handler_xptr);
}
