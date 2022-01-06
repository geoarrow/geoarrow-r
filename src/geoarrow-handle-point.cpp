#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "wk-v1.h"
#include "carrow.h"
#include "geoarrow-native.hpp"


SEXP geoarrow_read_native_point(SEXP data, wk_handler_t* handler) {
    struct ArrowArrayStream* array_stream = array_stream_from_xptr(VECTOR_ELT(data, 0), "handleable");
    struct ArrowSchema* schema = schema_from_xptr(VECTOR_ELT(data, 1), "schema");
    
    return R_NilValue;
}

SEXP geoarrow_read_native_point_struct(SEXP data, wk_handler_t* handler) {
    struct ArrowArrayStream* array_stream = array_stream_from_xptr(VECTOR_ELT(data, 0), "handleable");
    struct ArrowSchema* schema = schema_from_xptr(VECTOR_ELT(data, 1), "schema");

    return R_NilValue;
}

extern "C" SEXP geoarrow_c_handle_native_point(SEXP data, SEXP handler_xptr) {
  return wk_handler_run_xptr(&geoarrow_read_native_point, data, handler_xptr);
}

extern "C" SEXP geoarrow_c_handle_native_point_struct(SEXP data, SEXP handler_xptr) {
  return wk_handler_run_xptr(&geoarrow_read_native_point_struct, data, handler_xptr);
}
