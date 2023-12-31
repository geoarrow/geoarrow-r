
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "geoarrow.h"
#include "wk-v1.h"

#define RETURN_ABORT_NOT_OK(expr) \
  if ((expr) != GEOARROW_OK) {    \
    return WK_ABORT;              \
  }

typedef struct {
  struct GeoArrowArrayWriter writer;
  struct GeoArrowVisitor v;
  struct GeoArrowError error;
  struct GeoArrowCoordView coord_view;
  double coord[4];
  SEXP array_xptr;
} builder_handler_t;

int builder_vector_start(const wk_vector_meta_t* meta, void* handler_data) {
  return WK_CONTINUE;
}

SEXP builder_vector_end(const wk_vector_meta_t* meta, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;

  struct ArrowArray* array = (struct ArrowArray*)R_ExternalPtrAddr(data->array_xptr);
  int result = GeoArrowArrayWriterFinish(&data->writer, array, &data->error);
  if (result != GEOARROW_OK) {
    Rf_error("GeoArrowArrayWriterFinish() failed: %s", data->error.message);
  }

  return data->array_xptr;
}

int builder_feature_start(const wk_vector_meta_t* meta, R_xlen_t feat_id,
                          void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  RETURN_ABORT_NOT_OK(data->v.feat_start(&data->v));
  return WK_CONTINUE;
}

int builder_feature_null(void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  RETURN_ABORT_NOT_OK(data->v.null_feat(&data->v));
  return WK_CONTINUE;
}

int builder_feature_end(const wk_vector_meta_t* meta, R_xlen_t feat_id,
                        void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  RETURN_ABORT_NOT_OK(data->v.feat_end(&data->v));
  return WK_CONTINUE;
}

int builder_geometry_start(const wk_meta_t* meta, uint32_t part_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;

  enum GeoArrowGeometryType geometry_type =
      (enum GeoArrowGeometryType)meta->geometry_type;

  enum GeoArrowDimensions dimensions;
  if (meta->flags & WK_FLAG_HAS_Z && meta->flags & WK_FLAG_HAS_M) {
    dimensions = GEOARROW_DIMENSIONS_XYZM;
    data->coord_view.n_values = 4;
  } else if (meta->flags & WK_FLAG_HAS_Z) {
    dimensions = GEOARROW_DIMENSIONS_XYZ;
    data->coord_view.n_values = 3;
  } else if (meta->flags & WK_FLAG_HAS_M) {
    dimensions = GEOARROW_DIMENSIONS_XYM;
    data->coord_view.n_values = 3;
  } else {
    dimensions = GEOARROW_DIMENSIONS_XY;
    data->coord_view.n_values = 2;
  }

  data->coord_view.coords_stride = data->coord_view.n_values;

  RETURN_ABORT_NOT_OK(data->v.geom_start(&data->v, geometry_type, dimensions));
  return WK_CONTINUE;
}

int builder_geometry_end(const wk_meta_t* meta, uint32_t part_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  RETURN_ABORT_NOT_OK(data->v.geom_end(&data->v));
  return WK_CONTINUE;
}

int builder_ring_start(const wk_meta_t* meta, uint32_t size, uint32_t ring_id,
                       void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  RETURN_ABORT_NOT_OK(data->v.ring_start(&data->v));
  return WK_CONTINUE;
}

int builder_ring_end(const wk_meta_t* meta, uint32_t size, uint32_t ring_id,
                     void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  RETURN_ABORT_NOT_OK(data->v.ring_end(&data->v));
  return WK_CONTINUE;
}

int builder_coord(const wk_meta_t* meta, const double* coord, uint32_t coord_id,
                  void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  memcpy(data->coord, coord, data->coord_view.n_values * sizeof(double));
  RETURN_ABORT_NOT_OK(data->v.coords(&data->v, &data->coord_view));
  return WK_CONTINUE;
}

int builder_error(const char* message, void* handler_data) {
  Rf_error("%s", message);
  return WK_ABORT;
}

void builder_finalize(void* handler_data) {
  builder_handler_t* data = (builder_handler_t*)handler_data;
  if (data != NULL) {
    if (data->writer.private_data != NULL) {
      GeoArrowArrayWriterReset(&data->writer);
    }

    free(data);
  }
}

SEXP geoarrow_c_writer_new(SEXP schema_xptr, SEXP array_out_xptr) {
  wk_handler_t* handler = wk_handler_create();

  handler->vector_start = &builder_vector_start;
  handler->vector_end = &builder_vector_end;
  handler->feature_start = &builder_feature_start;
  handler->null_feature = &builder_feature_null;
  handler->feature_end = &builder_feature_end;
  handler->geometry_start = &builder_geometry_start;
  handler->geometry_end = &builder_geometry_end;
  handler->ring_start = &builder_ring_start;
  handler->ring_end = &builder_ring_end;
  handler->coord = &builder_coord;
  handler->error = &builder_error;
  handler->finalizer = &builder_finalize;

  builder_handler_t* data = (builder_handler_t*)malloc(sizeof(builder_handler_t));
  if (data == NULL) {
    wk_handler_destroy(handler);               // # nocov
    Rf_error("Failed to alloc handler data");  // # nocov
  }

  // Set up the writer
  struct ArrowSchema* schema = (struct ArrowSchema*)R_ExternalPtrAddr(schema_xptr);
  int result = GeoArrowArrayWriterInitFromSchema(&data->writer, schema);
  if (result != GEOARROW_OK) {
    free(data);
    Rf_error("GeoArrowArrayWriterInitFromSchema() fail");
  }

  result = GeoArrowArrayWriterInitVisitor(&data->writer, &data->v);
  if (result != GEOARROW_OK) {
    GeoArrowArrayWriterReset(&data->writer);
    free(data);
    Rf_error("GeoArrowArrayWriterInitVisitor() failed");
  }

  // Set up error reporting
  data->v.error = &data->error;
  data->error.message[0] = '\0';

  // Set up coord view
  data->coord_view.n_values = 4;
  data->coord_view.coords_stride = 4;
  data->coord_view.n_coords = 1;
  for (int i = 0; i < 4; i++) {
    data->coord_view.values[i] = data->coord + i;
  }

  // Set up the output
  data->array_xptr = array_out_xptr;

  handler->handler_data = data;
  return wk_handler_create_xptr(handler, array_out_xptr, R_NilValue);
}
