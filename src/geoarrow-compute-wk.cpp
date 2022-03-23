
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "wk-v1.h"
#include "narrow.h"
#include "geoarrow.h"
#include "util.h"


// The other versions of CPP_START and CPP_END stack-allocate the
// error message buffer, which takes a non-trivial amount of time
// when done at this scale (at worst 4 times per coordinate). By
// keeping the buffer in the handler_data struct, we can call C++
// from every handler method without measurable overhead.
#define WK_METHOD_CPP_START                     \
    try {

#define WK_METHOD_CPP_END                                 \
    } catch (std::exception& e) {                         \
        strncpy(data->cpp_exception_error, e.what(), 8096 - 1); \
    }                                                     \
    Rf_error("%s", data->cpp_exception_error);            \
    return R_NilValue;

#define WK_METHOD_CPP_END_INT                                 \
    } catch (std::exception& e) {                         \
        strncpy(data->cpp_exception_error, e.what(), 8096 - 1); \
    }                                                     \
    Rf_error("%s", data->cpp_exception_error);            \
    return WK_ABORT;


typedef struct {
    geoarrow::ComputeBuilder* builder;
    int coord_size;
    geoarrow::util::Dimensions dim;
    geoarrow::util::GeometryType geometry_type;
    SEXP array_sexp;
    char cpp_exception_error[8096];
} builder_handler_t;

int builder_vector_start(const wk_vector_meta_t* meta, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START

  data->geometry_type = static_cast<geoarrow::util::GeometryType>(meta->geometry_type);

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

  data->builder->new_schema(nullptr);
  data->builder->new_geometry_type(data->geometry_type);
  data->builder->new_dimensions(data->dim);

  if (data->builder->array_start(nullptr) == geoarrow::Handler::Result::ABORT_ARRAY) {
      return WK_ABORT;
  } else {
      return WK_CONTINUE;
  }
  WK_METHOD_CPP_END_INT
}

SEXP builder_vector_end(const wk_vector_meta_t* meta, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START
  data->builder->array_end();

  struct ArrowSchema* schema_out = reinterpret_cast<struct ArrowSchema*>(
    R_ExternalPtrAddr(VECTOR_ELT(data->array_sexp, 0)));
  struct ArrowArray* array_data_out = reinterpret_cast<struct ArrowArray*>(
    R_ExternalPtrAddr(VECTOR_ELT(data->array_sexp, 1)));

  data->builder->release(array_data_out, schema_out);
  return data->array_sexp;
  WK_METHOD_CPP_END
}

int builder_feature_start(const wk_vector_meta_t* meta, R_xlen_t feat_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START
  return static_cast<int>(data->builder->feat_start());
  WK_METHOD_CPP_END_INT
}

int builder_feature_null(void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START
  return static_cast<int>(data->builder->null_feat());
  WK_METHOD_CPP_END_INT
}

int builder_feature_end(const wk_vector_meta_t* meta, R_xlen_t feat_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START
  return static_cast<int>(data->builder->feat_end());
  WK_METHOD_CPP_END_INT
}

int builder_geometry_start(const wk_meta_t* meta, uint32_t part_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START

  auto geometry_type = static_cast<geoarrow::util::GeometryType>(meta->geometry_type);

  int32_t size;
  if (meta->size == WK_SIZE_UNKNOWN) {
      size = -1;
  } else {
      size = meta->size;
  }

  geoarrow::util::Dimensions new_dim;
  if (meta->flags & WK_FLAG_HAS_Z && meta->flags & WK_FLAG_HAS_M) {
    new_dim = geoarrow::util::Dimensions::XYZM;
    data->coord_size = 4;
  } else if (meta->flags & WK_FLAG_HAS_Z) {
    new_dim = geoarrow::util::Dimensions::XYZ;
    data->coord_size = 3;
  } else if (meta->flags & WK_FLAG_HAS_M) {
    new_dim = geoarrow::util::Dimensions::XYM;
    data->coord_size = 3;
  } else {
    new_dim = geoarrow::util::Dimensions::XY;
    data->coord_size = 2;
  }

  if (geometry_type != data->geometry_type) {
    data->builder->new_geometry_type(geometry_type);
    data->geometry_type = geometry_type;
  }

  if (new_dim != data->dim) {
    data->builder->new_dimensions(new_dim);
    data->dim = new_dim;
  }

  return static_cast<int>(data->builder->geom_start(geometry_type, size));
  WK_METHOD_CPP_END_INT
}

int builder_geometry_end(const wk_meta_t* meta, uint32_t part_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START
  return static_cast<int>(data->builder->geom_end());
  WK_METHOD_CPP_END_INT
}

int builder_ring_start(const wk_meta_t* meta, uint32_t size, uint32_t ring_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START

  if (size == WK_SIZE_UNKNOWN) {
      return static_cast<int>(data->builder->ring_start(-1));
  } else {
      return static_cast<int>(data->builder->ring_start(size));
  }
  WK_METHOD_CPP_END_INT
}

int builder_ring_end(const wk_meta_t* meta, uint32_t size, uint32_t ring_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START
  return static_cast<int>(data->builder->ring_end());
  WK_METHOD_CPP_END_INT
}

int builder_coord(const wk_meta_t* meta, const double* coord, uint32_t coord_id, void* handler_data) {
  builder_handler_t* data = (builder_handler_t*) handler_data;
  WK_METHOD_CPP_START
  return static_cast<int>(data->builder->coords(coord, 1, data->coord_size));
  WK_METHOD_CPP_END_INT
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

extern "C" SEXP geoarrow_c_compute_handler_new(SEXP op_sexp, SEXP schema_xptr, SEXP array_sexp_out) {
  CPP_START

  auto op = static_cast<geoarrow::compute::Operation>(INTEGER(op_sexp)[0]);
  if (op < 0 || op >= geoarrow::compute::Operation::OP_INVALID) {
      Rf_error("Unsupported operation: %d", op);
  }

  struct ArrowSchema* schema = schema_from_xptr(schema_xptr, "schema");
  geoarrow::ComputeBuilder* builder = geoarrow::create_builder(op, schema, 1024);

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

  data->coord_size = 2;
  data->dim = geoarrow::util::Dimensions::DIMENSIONS_UNKNOWN;
  data->geometry_type = geoarrow::util::GeometryType::GEOMETRY_TYPE_UNKNOWN;
  data->builder = builder;
  data->array_sexp = array_sexp_out;
  memset(data->cpp_exception_error, 0, 8096);

  handler->handler_data = data;

  // include the builder pointer as a tag for this external pointer
  // which guarnatees that it will not be garbage collected until
  // this object is garbage collected
  SEXP handler_xptr = wk_handler_create_xptr(handler, builder_xptr, array_sexp_out);
  UNPROTECT(1);
  return handler_xptr;

  CPP_END
}
