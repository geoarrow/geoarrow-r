
#include <string.h>

#include "nanoarrow.h"

#include "geoarrow.h"

struct WKBWriterPrivate {
  enum ArrowType storage_type;
  struct ArrowBitmap validity;
  struct ArrowBuffer offsets;
  struct ArrowBuffer values;
  enum GeoArrowGeometryType geometry_type[32];
  enum GeoArrowDimensions dimensions[32];
  int64_t size_pos[32];
  uint32_t size[32];
  int32_t level;
  int64_t length;
  int64_t null_count;
  int feat_is_null;
};

#ifndef GEOARROW_NATIVE_ENDIAN
#define GEOARROW_NATIVE_ENDIAN 0x01
#endif

static uint8_t kWKBWriterEmptyPointCoords2[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                0xf8, 0x7f, 0x00, 0x00, 0x00, 0x00,
                                                0x00, 0x00, 0xf8, 0x7f};
static uint8_t kWKBWriterEmptyPointCoords3[] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0xf8, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f};
static uint8_t kWKBWriterEmptyPointCoords4[] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0xf8, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xf8, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f};

static inline int WKBWriterCheckLevel(struct WKBWriterPrivate* private) {
  if (private->level >= 0 && private->level <= 30) {
    return GEOARROW_OK;
  } else {
    return EINVAL;
  }
}

static int feat_start_wkb(struct GeoArrowVisitor* v) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;
  private->level = 0;
  private->size[private->level] = 0;
  private->length++;
  private->feat_is_null = 0;

  if (private->values.size_bytes > 2147483647) {
    return EOVERFLOW;
  }
  return ArrowBufferAppendInt32(&private->offsets, (int32_t) private->values.size_bytes);
}

static int null_feat_wkb(struct GeoArrowVisitor* v) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;
  private->feat_is_null = 1;
  return GEOARROW_OK;
}

static int geom_start_wkb(struct GeoArrowVisitor* v,
                          enum GeoArrowGeometryType geometry_type,
                          enum GeoArrowDimensions dimensions) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKBWriterCheckLevel(private));
  private->size[private->level]++;
  private->level++;
  private->geometry_type[private->level] = geometry_type;
  private->dimensions[private->level] = dimensions;
  private->size[private->level] = 0;

  NANOARROW_RETURN_NOT_OK(
      ArrowBufferAppendUInt8(&private->values, GEOARROW_NATIVE_ENDIAN));
  NANOARROW_RETURN_NOT_OK(ArrowBufferAppendUInt32(
      &private->values, geometry_type + ((dimensions - 1) * 1000)));
  if (geometry_type != GEOARROW_GEOMETRY_TYPE_POINT) {
    private->size_pos[private->level] = private->values.size_bytes;
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppendUInt32(&private->values, 0));
  }

  return GEOARROW_OK;
}

static int ring_start_wkb(struct GeoArrowVisitor* v) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKBWriterCheckLevel(private));
  private->size[private->level]++;
  private->level++;
  private->geometry_type[private->level] = GEOARROW_GEOMETRY_TYPE_GEOMETRY;
  private->size_pos[private->level] = private->values.size_bytes;
  private->size[private->level] = 0;
  return ArrowBufferAppendUInt32(&private->values, 0);
}

static int coords_wkb(struct GeoArrowVisitor* v, const struct GeoArrowCoordView* coords) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKBWriterCheckLevel(private));
  private->size[private->level] += coords->n_coords;
  NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(
      &private->values, coords->n_values * coords->n_coords * sizeof(double)));
  for (int64_t i = 0; i < coords->n_coords; i++) {
    for (int32_t j = 0; j < coords->n_values; j++) {
      ArrowBufferAppendUnsafe(&private->values,
                              coords->values[j] + i * coords->coords_stride,
                              sizeof(double));
    }
  }

  return GEOARROW_OK;
}

static int ring_end_wkb(struct GeoArrowVisitor* v) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKBWriterCheckLevel(private));
  if (private->values.data == NULL) {
    return EINVAL;
  }
  memcpy(private->values.data + private->size_pos[private->level],
         private->size + private->level, sizeof(uint32_t));
  private->level--;
  return GEOARROW_OK;
}

static int geom_end_wkb(struct GeoArrowVisitor* v) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;
  NANOARROW_RETURN_NOT_OK(WKBWriterCheckLevel(private));
  if (private->values.data == NULL) {
    return EINVAL;
  }

  if (private->geometry_type[private->level] != GEOARROW_GEOMETRY_TYPE_POINT) {
    memcpy(private->values.data + private->size_pos[private->level],
           private->size + private->level, sizeof(uint32_t));
  } else if (private->size[private->level] == 0) {
    switch (private->dimensions[private->level]) {
      case GEOARROW_DIMENSIONS_XY:
        NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&private->values,
                                                  kWKBWriterEmptyPointCoords2,
                                                  sizeof(kWKBWriterEmptyPointCoords2)));
        break;
      case GEOARROW_DIMENSIONS_XYZ:
      case GEOARROW_DIMENSIONS_XYM:
        NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&private->values,
                                                  kWKBWriterEmptyPointCoords3,
                                                  sizeof(kWKBWriterEmptyPointCoords3)));
        break;
      case GEOARROW_DIMENSIONS_XYZM:
        NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&private->values,
                                                  kWKBWriterEmptyPointCoords4,
                                                  sizeof(kWKBWriterEmptyPointCoords4)));
        break;
      default:
        return EINVAL;
    }
  }

  private->level--;
  return GEOARROW_OK;
}

static int feat_end_wkb(struct GeoArrowVisitor* v) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)v->private_data;

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

  return GEOARROW_OK;
}

GeoArrowErrorCode GeoArrowWKBWriterInit(struct GeoArrowWKBWriter* writer) {
  struct WKBWriterPrivate* private =
      (struct WKBWriterPrivate*)ArrowMalloc(sizeof(struct WKBWriterPrivate));
  if (private == NULL) {
    return ENOMEM;
  }

  private->storage_type = NANOARROW_TYPE_BINARY;
  private->length = 0;
  private->level = 0;
  private->null_count = 0;
  ArrowBitmapInit(&private->validity);
  ArrowBufferInit(&private->offsets);
  ArrowBufferInit(&private->values);
  writer->private_data = private;

  return GEOARROW_OK;
}

void GeoArrowWKBWriterInitVisitor(struct GeoArrowWKBWriter* writer,
                                  struct GeoArrowVisitor* v) {
  GeoArrowVisitorInitVoid(v);

  v->private_data = writer->private_data;
  v->feat_start = &feat_start_wkb;
  v->null_feat = &null_feat_wkb;
  v->geom_start = &geom_start_wkb;
  v->ring_start = &ring_start_wkb;
  v->coords = &coords_wkb;
  v->ring_end = &ring_end_wkb;
  v->geom_end = &geom_end_wkb;
  v->feat_end = &feat_end_wkb;
}

GeoArrowErrorCode GeoArrowWKBWriterFinish(struct GeoArrowWKBWriter* writer,
                                          struct ArrowArray* array,
                                          struct GeoArrowError* error) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)writer->private_data;
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

void GeoArrowWKBWriterReset(struct GeoArrowWKBWriter* writer) {
  struct WKBWriterPrivate* private = (struct WKBWriterPrivate*)writer->private_data;
  ArrowBitmapReset(&private->validity);
  ArrowBufferReset(&private->offsets);
  ArrowBufferReset(&private->values);
  ArrowFree(private);
  writer->private_data = NULL;
}
