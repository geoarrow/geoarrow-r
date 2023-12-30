
#include <errno.h>
#include <string.h>

#include "nanoarrow.h"

#include "geoarrow.h"

static GeoArrowErrorCode GeoArrowSchemaInitCoordFixedSizeList(struct ArrowSchema* schema,
                                                              const char* dims) {
  int64_t n_dims = strlen(dims);
  ArrowSchemaInit(schema);
  NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeFixedSize(
      schema, NANOARROW_TYPE_FIXED_SIZE_LIST, (int32_t)n_dims));
  NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema->children[0], dims));
  NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_DOUBLE));

  return GEOARROW_OK;
}

static GeoArrowErrorCode GeoArrowSchemaInitCoordStruct(struct ArrowSchema* schema,
                                                       const char* dims) {
  int64_t n_dims = strlen(dims);
  char dim_name[] = {'\0', '\0'};
  NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, NANOARROW_TYPE_STRUCT));
  NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateChildren(schema, n_dims));
  for (int64_t i = 0; i < n_dims; i++) {
    dim_name[0] = dims[i];
    NANOARROW_RETURN_NOT_OK(
        ArrowSchemaInitFromType(schema->children[i], NANOARROW_TYPE_DOUBLE));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema->children[i], dim_name));
  }

  return GEOARROW_OK;
}

static GeoArrowErrorCode GeoArrowSchemaInitListStruct(struct ArrowSchema* schema,
                                                      enum GeoArrowCoordType coord_type,
                                                      const char* dims, int n,
                                                      const char** child_names) {
  if (n == 0) {
    switch (coord_type) {
      case GEOARROW_COORD_TYPE_SEPARATE:
        return GeoArrowSchemaInitCoordStruct(schema, dims);
      case GEOARROW_COORD_TYPE_INTERLEAVED:
        return GeoArrowSchemaInitCoordFixedSizeList(schema, dims);
      default:
        return EINVAL;
    }
  } else {
    ArrowSchemaInit(schema);
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetFormat(schema, "+l"));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateChildren(schema, 1));
    NANOARROW_RETURN_NOT_OK(GeoArrowSchemaInitListStruct(schema->children[0], coord_type,
                                                         dims, n - 1, child_names + 1));
    return ArrowSchemaSetName(schema->children[0], child_names[0]);
  }
}

#define CHILD_NAMES_LINESTRING \
  (const char*[]) { "vertices" }
#define CHILD_NAMES_POLYGON \
  (const char*[]) { "rings", "vertices" }
#define CHILD_NAMES_MULTIPOINT \
  (const char*[]) { "points" }
#define CHILD_NAMES_MULTILINESTRING \
  (const char*[]) { "linestrings", "vertices" }
#define CHILD_NAMES_MULTIPOLYGON \
  (const char*[]) { "polygons", "rings", "vertices" }

GeoArrowErrorCode GeoArrowSchemaInit(struct ArrowSchema* schema, enum GeoArrowType type) {
  schema->release = NULL;

  switch (type) {
    case GEOARROW_TYPE_WKB:
      return ArrowSchemaInitFromType(schema, NANOARROW_TYPE_BINARY);
    case GEOARROW_TYPE_LARGE_WKB:
      return ArrowSchemaInitFromType(schema, NANOARROW_TYPE_LARGE_BINARY);

    case GEOARROW_TYPE_WKT:
      return ArrowSchemaInitFromType(schema, NANOARROW_TYPE_STRING);
    case GEOARROW_TYPE_LARGE_WKT:
      return ArrowSchemaInitFromType(schema, NANOARROW_TYPE_LARGE_STRING);

    default:
      break;
  }

  enum GeoArrowDimensions dimensions = GeoArrowDimensionsFromType(type);
  enum GeoArrowCoordType coord_type = GeoArrowCoordTypeFromType(type);
  enum GeoArrowGeometryType geometry_type = GeoArrowGeometryTypeFromType(type);

  const char* dims;
  switch (dimensions) {
    case GEOARROW_DIMENSIONS_XY:
      dims = "xy";
      break;
    case GEOARROW_DIMENSIONS_XYZ:
      dims = "xyz";
      break;
    case GEOARROW_DIMENSIONS_XYM:
      dims = "xym";
      break;
    case GEOARROW_DIMENSIONS_XYZM:
      dims = "xyzm";
      break;
    default:
      return EINVAL;
  }

  switch (geometry_type) {
    case GEOARROW_GEOMETRY_TYPE_POINT:
      switch (coord_type) {
        case GEOARROW_COORD_TYPE_SEPARATE:
          return GeoArrowSchemaInitCoordStruct(schema, dims);
        case GEOARROW_COORD_TYPE_INTERLEAVED:
          return GeoArrowSchemaInitCoordFixedSizeList(schema, dims);
        default:
          return EINVAL;
      }

    case GEOARROW_GEOMETRY_TYPE_LINESTRING:
      return GeoArrowSchemaInitListStruct(schema, coord_type, dims, 1,
                                          CHILD_NAMES_LINESTRING);
    case GEOARROW_GEOMETRY_TYPE_MULTIPOINT:
      return GeoArrowSchemaInitListStruct(schema, coord_type, dims, 1,
                                          CHILD_NAMES_MULTIPOINT);
    case GEOARROW_GEOMETRY_TYPE_POLYGON:
      return GeoArrowSchemaInitListStruct(schema, coord_type, dims, 2,
                                          CHILD_NAMES_POLYGON);
    case GEOARROW_GEOMETRY_TYPE_MULTILINESTRING:
      return GeoArrowSchemaInitListStruct(schema, coord_type, dims, 2,
                                          CHILD_NAMES_MULTILINESTRING);
    case GEOARROW_GEOMETRY_TYPE_MULTIPOLYGON:
      return GeoArrowSchemaInitListStruct(schema, coord_type, dims, 3,
                                          CHILD_NAMES_MULTIPOLYGON);

    default:
      return ENOTSUP;
  }
}

GeoArrowErrorCode GeoArrowSchemaInitExtension(struct ArrowSchema* schema,
                                              enum GeoArrowType type) {
  const char* ext_type = GeoArrowExtensionNameFromType(type);
  if (ext_type == NULL) {
    return EINVAL;
  }

  struct ArrowBuffer metadata;
  NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(&metadata, NULL));
  int result = ArrowMetadataBuilderAppend(
      &metadata, ArrowCharView("ARROW:extension:name"), ArrowCharView(ext_type));
  if (result != NANOARROW_OK) {
    ArrowBufferReset(&metadata);
    return result;
  }

  result = GeoArrowSchemaInit(schema, type);
  if (result != NANOARROW_OK) {
    ArrowBufferReset(&metadata);
    return result;
  }

  result = ArrowSchemaSetMetadata(schema, (const char*)metadata.data);
  ArrowBufferReset(&metadata);
  return result;
}
