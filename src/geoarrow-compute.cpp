
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "narrow.h"
#include "geoarrow.h"
#include "util.h"
#include "compute-util.h"

#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == geoarrow::Handler::Result::ABORT_FEATURE) \
        continue; \
    else if (result == geoarrow::Handler::Result::ABORT) break


extern "C" SEXP geoarrow_c_compute(SEXP op_sexp,
                                   SEXP array_from_sexp,
                                   SEXP array_to_sexp,
                                   SEXP filter_sexp,
                                   SEXP options_sexp) {
    CPP_START

    const char* op = Rf_translateCharUTF8(STRING_ELT(op_sexp, 0));

    SEXP options_xptr = PROTECT(compute_options_from_sexp(options_sexp));
    auto options = reinterpret_cast<geoarrow::ComputeOptions*>(R_ExternalPtrAddr(options_xptr));

    struct ArrowSchema* schema_from = schema_from_xptr(
        VECTOR_ELT(array_from_sexp, 0),
        "array$schema");
    struct ArrowArray* array_data_from = array_data_from_xptr(
        VECTOR_ELT(array_from_sexp, 1),
        "array$array_data");

    struct ArrowSchema* schema_to = reinterpret_cast<struct ArrowSchema*>(
        R_ExternalPtrAddr(VECTOR_ELT(array_to_sexp, 0)));
    struct ArrowArray* array_data_to = reinterpret_cast<struct ArrowArray*>(
        R_ExternalPtrAddr(VECTOR_ELT(array_to_sexp, 1)));

    // Get the ArrayView to read array_from
    geoarrow::ArrayView* view = geoarrow::create_view(schema_from);
    SEXP view_xptr = PROTECT(R_MakeExternalPtr(view, R_NilValue, R_NilValue));
    R_RegisterCFinalizer(view_xptr, &delete_array_view_xptr);

    // Get the builder to build array_to
    geoarrow::ComputeBuilder* builder = geoarrow::create_builder(op, *options);
    SEXP builder_xptr = PROTECT(R_MakeExternalPtr(builder, array_to_sexp, options_xptr));
    R_RegisterCFinalizer(builder_xptr, &delete_array_builder_xptr);

    // Do the compute operation on a (possible) subset of the array
    view->read_meta(builder);
    view->set_array(array_data_from);

    if (TYPEOF(filter_sexp) == LGLSXP &&
        Rf_length(filter_sexp) == 1 &&
        LOGICAL(filter_sexp)[0] == 1) {
        view->read_features(builder);
    } else if (TYPEOF(filter_sexp) == LGLSXP &&
               Rf_xlength(filter_sexp) == array_data_from->length) {
        // for now, don't worry about ALTREP
        geoarrow::Handler::Result result;
        int* filter = LOGICAL(filter_sexp);

        for (int64_t i = 0; i < array_data_from->length; i++) {
            if (filter[i] == NA_LOGICAL) {
                HANDLE_CONTINUE_OR_BREAK(builder->feat_start());
                HANDLE_CONTINUE_OR_BREAK(builder->null_feat());
                HANDLE_CONTINUE_OR_BREAK(builder->feat_end());
            } else if (filter[i]) {
                HANDLE_CONTINUE_OR_BREAK(view->read_feature(builder, i));
            }
        }
    } else if (TYPEOF(filter_sexp) == INTSXP) {
        // for now, don't worry about ALTREP
        geoarrow::Handler::Result result;
        int* filter = INTEGER(filter_sexp);
        R_xlen_t n_filter = Rf_xlength(filter_sexp);

        for (R_xlen_t i = 0; i < n_filter; i++) {
            if (filter[i] == NA_INTEGER ||
                filter[i] < 1 ||
                filter[i] > array_data_from->length) {
                HANDLE_CONTINUE_OR_BREAK(builder->feat_start());
                HANDLE_CONTINUE_OR_BREAK(builder->null_feat());
                HANDLE_CONTINUE_OR_BREAK(builder->feat_end());
            } else {
                HANDLE_CONTINUE_OR_BREAK(view->read_feature(builder, filter[i] - 1));
            }
        }
    } else if (TYPEOF(filter_sexp) == REALSXP) {
        // for now, don't worry about ALTREP
        geoarrow::Handler::Result result;
        double* filter = REAL(filter_sexp);
        R_xlen_t n_filter = Rf_xlength(filter_sexp);

        for (R_xlen_t i = 0; i < n_filter; i++) {
            if (ISNA(filter[i]) || ISNAN(filter[i]) ||
                filter[i] < 1 ||
                filter[i] > array_data_from->length) {
                HANDLE_CONTINUE_OR_BREAK(builder->feat_start());
                HANDLE_CONTINUE_OR_BREAK(builder->null_feat());
                HANDLE_CONTINUE_OR_BREAK(builder->feat_end());
            } else {
                HANDLE_CONTINUE_OR_BREAK(view->read_feature(builder, filter[i] - 1));
            }
        }
    } else {
        Rf_error("Filter type not supported");
    }

    // Transfer ownership of the built array_data and schema to array_to
    builder->release(array_data_to, schema_to);

    // The pointers pointed to by array_to have been modified in place,
    // but return it anyway in case we do something else in the future
    UNPROTECT(3);
    return array_to_sexp;
    CPP_END
}
