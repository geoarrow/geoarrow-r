#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "wk-v1.h"
#include "carrow.h"
#include "geoarrow-native.hpp"
#include "util.h"


SEXP geoarrow_read_point(SEXP data, wk_handler_t* handler) {
    struct ArrowArrayStream* array_stream = array_stream_from_xptr(VECTOR_ELT(data, 0), "handleable");
    struct ArrowSchema* schema = schema_from_xptr(VECTOR_ELT(data, 1), "schema");
    SEXP n_features_sexp = VECTOR_ELT(data, 2);

    GeoArrowPointView view(schema);

    if (TYPEOF(n_features_sexp) == INTSXP) {
        if (INTEGER(n_features_sexp)[0] != NA_INTEGER) {
            view.set_vector_size(INTEGER(n_features_sexp)[0]);
        }
    } else {
        double n_features_double = REAL(n_features_sexp)[0];
        if (!ISNA(n_features_double) && !ISNAN(n_features_double)) {
            view.set_vector_size(n_features_double);
        }
    }

    int result = handler->vector_start(&view.vector_meta_, handler->handler_data);
    if (result == WK_CONTINUE) {
        struct ArrowArray* array_data = (struct ArrowArray*) malloc(sizeof(struct ArrowArray));
        if (array_data == NULL) {
            Rf_error("Failed to allocate struct ArrowArray");
        }
        array_data->release = NULL;
        SEXP array_data_wrapper = PROTECT(R_MakeExternalPtr(array_data, R_NilValue, R_NilValue));
        R_RegisterCFinalizer(array_data_wrapper, &geoarrow_finalize_array_data);

        int stream_result = 0;
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

            for (int64_t i = array_data->offset; i < array_data->length; i++) {
                if (((i + 1) % 1000) == 0) R_CheckUserInterrupt();
                HANDLE_CONTINUE_OR_BREAK(view.read_feature(handler));
            }
        }

        UNPROTECT(1);
    }

    SEXP result_sexp = PROTECT(handler->vector_end(&view.vector_meta_, handler->handler_data));
    UNPROTECT(1);
    return result_sexp;
    
    return R_NilValue;
}


extern "C" SEXP geoarrow_c_handle_point(SEXP data, SEXP handler_xptr) {
  return wk_handler_run_xptr(&geoarrow_read_point, data, handler_xptr);
}
