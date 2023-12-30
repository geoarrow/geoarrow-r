
#include <stdio.h>
#include <string.h>

#include "nanoarrow.h"

#include "geoarrow.h"

struct WKTWriterPrivate {
  enum ArrowType storage_type;
  struct ArrowBitmap validity;
  struct ArrowBuffer offsets;
  struct ArrowBuffer values;
  enum GeoArrowGeometryType geometry_type[32];
  int64_t i[32];
  int32_t level;
  int64_t length;
  int64_t null_count;
  int64_t values_feat_start;
  int precision;
  int use_flat_multipoint;
  int64_t max_element_size_bytes;
  int feat_is_null;
};

static inline int WKTWriterCheckLevel(struct WKTWriterPrivate* private) {
  if (private->level >= 0 && private->level <= 31) {
    return GEOARROW_OK;
  } else {
    return EINVAL;
  }
}

static inline int WKTWriterWrite(struct WKTWriterPrivate* private, const char* value) {
  return ArrowBufferAppend(&private->values, value, strlen(value));
}

static inline void WKTWriterWriteDoubleUnsafe(struct WKTWriterPrivate* private,
                                              double value) {
  private->values.size_bytes +=
      GeoArrowPrintDouble(value, private->precision,
                          ((char*)private->values.data) + private->values.size_bytes);
}

static int feat_start_wkt(struct GeoArrowVisitor* v) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;
  private->level = -1;
  private->length++;
  private->feat_is_null = 0;
  private->values_feat_start = private->values.size_bytes;

  if (private->values.size_bytes > 2147483647) {
    return EOVERFLOW;
  }
  return ArrowBufferAppendInt32(&private->offsets, (int32_t) private->values.size_bytes);
}

static int null_feat_wkt(struct GeoArrowVisitor* v) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;
  private->feat_is_null = 1;
  return GEOARROW_OK;
}

static int geom_start_wkt(struct GeoArrowVisitor* v,
                          enum GeoArrowGeometryType geometry_type,
                          enum GeoArrowDimensions dimensions) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;
  private->level++;
  NANOARROW_RETURN_NOT_OK(WKTWriterCheckLevel(private));

  if (private->level > 0 && private->i[private->level - 1] > 0) {
    NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, ", "));
  } else if (private->level > 0) {
    NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, "("));
  }

  if (private->level == 0 || private->geometry_type[private->level - 1] ==
                                 GEOARROW_GEOMETRY_TYPE_GEOMETRYCOLLECTION) {
    const char* geometry_type_name = GeoArrowGeometryTypeString(geometry_type);
    if (geometry_type_name == NULL) {
      GeoArrowErrorSet(v->error, "WKTWriter::geom_start(): Unexpected `geometry_type`");
      return EINVAL;
    }

    NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, geometry_type_name));

    switch (dimensions) {
      case GEOARROW_DIMENSIONS_XY:
        break;
      case GEOARROW_DIMENSIONS_XYZ:
        NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, " Z"));
        break;
      case GEOARROW_DIMENSIONS_XYM:
        NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, " M"));
        break;
      case GEOARROW_DIMENSIONS_XYZM:
        NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, " ZM"));
        break;
      default:
        GeoArrowErrorSet(v->error, "WKTWriter::geom_start(): Unexpected `dimensions`");
        return EINVAL;
    }

    NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, " "));
  }

  if (private->level > 0) {
    private->i[private->level - 1]++;
  }

  private->geometry_type[private->level] = geometry_type;
  private->i[private->level] = 0;
  return GEOARROW_OK;
}

static int ring_start_wkt(struct GeoArrowVisitor* v) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;
  private->level++;
  NANOARROW_RETURN_NOT_OK(WKTWriterCheckLevel(private));

  if (private->level > 0 && private->i[private->level - 1] > 0) {
    NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, ", "));
  } else {
    NANOARROW_RETURN_NOT_OK(WKTWriterWrite(private, "("));
  }

  if (private->level > 0) {
    private->i[private->level - 1]++;
  }

  private->geometry_type[private->level] = GEOARROW_GEOMETRY_TYPE_GEOMETRY;
  private->i[private->level] = 0;
  return GEOARROW_OK;
}

static int coords_wkt(struct GeoArrowVisitor* v, const struct GeoArrowCoordView* coords) {
  int64_t n_coords = coords->n_coords;
  int32_t n_dims = coords->n_values;
  if (n_coords == 0) {
    return GEOARROW_OK;
  }

  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKTWriterCheckLevel(private));

  int64_t max_chars_needed = (n_coords * 2) +  // space + comma after coordinate
                             (n_coords * (n_dims - 1)) +  // spaces between ordinates
                             ((private->precision + 1 + 5) * n_coords *
                              n_dims);  // significant digits + decimal + exponent
  if (private->max_element_size_bytes >= 0 &&
      max_chars_needed > private->max_element_size_bytes) {
    // Because we write a coordinate before actually checking
    max_chars_needed = private->max_element_size_bytes + 1024;
  }

  NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(&private->values, max_chars_needed));

  // Write the first coordinate, possibly with a leading comma if there was
  // a previous call to coords, or the opening ( if it wasn't. Special case
  // for the flat multipoint output MULTIPOINT (1 2, 3 4, ...) which doesn't
  // have extra () for inner POINTs
  if (private->i[private->level] != 0) {
    ArrowBufferAppendUnsafe(&private->values, ", ", 2);
  } else if (private->level < 1 || !private->use_flat_multipoint ||
             private->geometry_type[private->level - 1] !=
                 GEOARROW_GEOMETRY_TYPE_MULTIPOINT) {
    ArrowBufferAppendUnsafe(&private->values, "(", 1);
  }

  WKTWriterWriteDoubleUnsafe(private, coords->values[0][0]);
  for (int32_t j = 1; j < n_dims; j++) {
    ArrowBufferAppendUnsafe(&private->values, " ", 1);
    WKTWriterWriteDoubleUnsafe(private, coords->values[j][0]);
  }

  // Write the remaining coordinates (which all have leading commas)
  for (int64_t i = 1; i < n_coords; i++) {
    if (private->max_element_size_bytes >= 0 &&
        (private->values.size_bytes - private->values_feat_start) >=
            private->max_element_size_bytes) {
      return EAGAIN;
    }

    ArrowBufferAppendUnsafe(&private->values, ", ", 2);
    WKTWriterWriteDoubleUnsafe(private, coords->values[0][i * coords->coords_stride]);
    for (int32_t j = 1; j < n_dims; j++) {
      ArrowBufferAppendUnsafe(&private->values, " ", 1);
      WKTWriterWriteDoubleUnsafe(private, coords->values[j][i * coords->coords_stride]);
    }
  }

  private->i[private->level] += n_coords;
  return GEOARROW_OK;
}

static int ring_end_wkt(struct GeoArrowVisitor* v) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKTWriterCheckLevel(private));
  if (private->i[private->level] == 0) {
    private->level--;
    return WKTWriterWrite(private, "EMPTY");
  } else {
    private->level--;
    return WKTWriterWrite(private, ")");
  }
}

static int geom_end_wkt(struct GeoArrowVisitor* v) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKTWriterCheckLevel(private));

  if (private->i[private->level] == 0) {
    private->level--;
    return WKTWriterWrite(private, "EMPTY");
  } else if (private->level < 1 || !private->use_flat_multipoint ||
             private->geometry_type[private->level - 1] !=
                 GEOARROW_GEOMETRY_TYPE_MULTIPOINT) {
    private->level--;
    return WKTWriterWrite(private, ")");
  } else {
    private->level--;
    return GEOARROW_OK;
  }
}

static int feat_end_wkt(struct GeoArrowVisitor* v) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)v->private_data;

  if (private->feat_is_null) {
    if (private->validity.buffer.data == NULL) {
      NANOARROW_RETURN_NOT_OK(ArrowBitmapReserve(&private->validity, private->length));
      ArrowBitmapAppendUnsafe(&private->validity, 1, private->length - 1);
    }

    private->null_count++;
    return ArrowBitmapAppend(&private->validity, 0, 1);
  } else if (private->validity.buffer.data != NULL) {
    return ArrowBitmapAppend(&private->validity, 1, 1);
  }

  if (private->max_element_size_bytes >= 0 &&
      (private->values.size_bytes - private->values_feat_start) >
          private->max_element_size_bytes) {
    private->values.size_bytes =
        private->values_feat_start + private->max_element_size_bytes;
  }

  return GEOARROW_OK;
}

GeoArrowErrorCode GeoArrowWKTWriterInit(struct GeoArrowWKTWriter* writer) {
  struct WKTWriterPrivate* private =
      (struct WKTWriterPrivate*)ArrowMalloc(sizeof(struct WKTWriterPrivate));
  if (private == NULL) {
    return ENOMEM;
  }

  private->storage_type = NANOARROW_TYPE_STRING;
  private->length = 0;
  private->level = 0;
  private->null_count = 0;
  ArrowBitmapInit(&private->validity);
  ArrowBufferInit(&private->offsets);
  ArrowBufferInit(&private->values);
  writer->precision = 16;
  private->precision = 16;
  writer->use_flat_multipoint = 1;
  private->use_flat_multipoint = 1;
  writer->max_element_size_bytes = -1;
  private->max_element_size_bytes = -1;
  writer->private_data = private;

  return GEOARROW_OK;
}

void GeoArrowWKTWriterInitVisitor(struct GeoArrowWKTWriter* writer,
                                  struct GeoArrowVisitor* v) {
  GeoArrowVisitorInitVoid(v);

  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)writer->private_data;
  private->precision = writer->precision;
  private->use_flat_multipoint = writer->use_flat_multipoint;
  private->max_element_size_bytes = writer->max_element_size_bytes;

  v->private_data = writer->private_data;
  v->feat_start = &feat_start_wkt;
  v->null_feat = &null_feat_wkt;
  v->geom_start = &geom_start_wkt;
  v->ring_start = &ring_start_wkt;
  v->coords = &coords_wkt;
  v->ring_end = &ring_end_wkt;
  v->geom_end = &geom_end_wkt;
  v->feat_end = &feat_end_wkt;
}

GeoArrowErrorCode GeoArrowWKTWriterFinish(struct GeoArrowWKTWriter* writer,
                                          struct ArrowArray* array,
                                          struct GeoArrowError* error) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)writer->private_data;
  array->release = NULL;

  if (private->values.size_bytes > 2147483647) {
    return EOVERFLOW;
  }
  NANOARROW_RETURN_NOT_OK(
      ArrowBufferAppendInt32(&private->offsets, (int32_t) private->values.size_bytes));
  NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromType(array, private->storage_type));
  ArrowArraySetValidityBitmap(array, &private->validity);
  NANOARROW_RETURN_NOT_OK(ArrowArraySetBuffer(array, 1, &private->offsets));
  NANOARROW_RETURN_NOT_OK(ArrowArraySetBuffer(array, 2, &private->values));
  array->length = private->length;
  array->null_count = private->null_count;
  private->length = 0;
  private->null_count = 0;
  return ArrowArrayFinishBuildingDefault(array, (struct ArrowError*)error);
}

void GeoArrowWKTWriterReset(struct GeoArrowWKTWriter* writer) {
  struct WKTWriterPrivate* private = (struct WKTWriterPrivate*)writer->private_data;
  ArrowBitmapReset(&private->validity);
  ArrowBufferReset(&private->offsets);
  ArrowBufferReset(&private->values);
  ArrowFree(private);
  writer->private_data = NULL;
}
