
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "wk-v1.h"
#include "narrow.h"
#include "geoarrow.h"
#include "util.h"

typedef struct {
    geoarrow::GeoArrayBuilder* builder;
    int coord_size;
    geoarrow::util::Dimensions dim;
} builder_handler_t;

int builder_vector_start(const wk_vector_meta_t* meta, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  data->builder->new_geometry_type(
      static_cast<geoarrow::util::GeometryType>(meta->geometry_type));

  if (meta->flags & WK_FLAG_DIMS_UNKNOWN) {
      data->dim = geoarrow::util::Dimensions::DIMENSIONS_UNKNOWN;
  } else if (meta->flags & WK_FLAG_HAS_Z && meta->flags & WK_FLAG_HAS_M) {
      data->dim = geoarrow::util::Dimensions::XYZM;
  } else if (meta->flags & WK_FLAG_HAS_Z) {
      data->dim = geoarrow::util::Dimensions::XYZ;
  } else if (meta->flags & WK_FLAG_HAS_M) {
      data->dim = geoarrow::util::Dimensions::XYM;
  } else {
      data->dim = geoarrow::util::Dimensions::XY;
  }

  data->builder->new_dimensions(data->dim);
  data->builder->new_schema(nullptr);
  if (data->builder->array_start(nullptr) == geoarrow::Handler::Result::ABORT_ARRAY) {
      return WK_ABORT;
  } else {
      return WK_CONTINUE;
  }
}

SEXP builder_vector_end(const wk_vector_meta_t* meta, void* handler_data) {
    builder_handler_t* data = (builder_handler_t*) handler_data;
    data->builder->array_end();

    // TODO: build the thing and return it!

    return R_NilValue;
}

int builder_feature_start(const wk_vector_meta_t* meta, R_xlen_t feat_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  return static_cast<int>(data->builder->feat_start());
}

int builder_feature_null(void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  return static_cast<int>(data->builder->null_feat());
}

int builder_feature_end(const wk_vector_meta_t* meta, R_xlen_t feat_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  return static_cast<int>(data->builder->feat_end());
}

int builder_geometry_start(const wk_meta_t* meta, uint32_t part_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  auto geometry_type = static_cast<geoarrow::util::GeometryType>(meta->geometry_type);
  int32_t size;
  if (meta->size == WK_SIZE_UNKNOWN) {
      size = -1;
  } else {
      size = meta->size;
  }

  return static_cast<int>(data->builder->geom_start(geometry_type, size));
}

int builder_geometry_end(const wk_meta_t* meta, uint32_t part_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  return static_cast<int>(data->builder->geom_end());
}

int builder_ring_start(const wk_meta_t* meta, uint32_t size, uint32_t ring_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  if (size == WK_SIZE_UNKNOWN) {
      return static_cast<int>(data->builder->ring_start(-1));
  } else {
      return static_cast<int>(data->builder->ring_start(size));
  }
}

int builder_ring_end(const wk_meta_t* meta, uint32_t size, uint32_t ring_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  return static_cast<int>(data->builder->ring_end());
}

int builder_coord(const wk_meta_t* meta, const double* coord, uint32_t coord_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  // TODO: actual coord size here
  return static_cast<int>(data->builder->coords(coord, 1, 2));
}

int builder_error(const char* message, void* handler_data) {
  Rf_error("%s", message);
  return WK_ABORT;
}

void builder_finalize(void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  if (data != nullptr) {
    free(data);
  }
}

extern "C" SEXP geoarrow_c_builder_handler_new(SEXP schema_xptr) {
  CPP_START

  struct ArrowSchema* schema = schema_from_xptr(schema_xptr, "schema");
  geoarrow::GeoArrayBuilder* builder = geoarrow::create_builder(schema, 1024);

  // Use an external pointer to make sure the builder and its data are
  // cleanded up.
  SEXP builder_xptr = PROTECT(R_MakeExternalPtr(builder, schema_xptr, R_NilValue));
  R_RegisterCFinalizer(builder_xptr, &delete_array_builder_xptr);

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

  builder_handler_t* data = (builder_handler_t*) malloc(sizeof(builder_handler_t));
  if (data == NULL) {
    wk_handler_destroy(handler); // # nocov
    Rf_error("Failed to alloc handler data"); // # nocov
  }

  data->builder = builder;
  handler->handler_data = data;

  // include the builder pointer as a tag for this external pointer
  // which guarnatees that it will not be garbage collected until
  // this object is garbage collected
  SEXP handler_xptr = wk_handler_create_xptr(handler, builder_xptr, R_NilValue);
  UNPROTECT(1);
  return handler_xptr;

  CPP_END
}
