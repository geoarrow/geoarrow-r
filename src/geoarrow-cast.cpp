
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "narrow.h"
#include "geoarrow.h"
#include "util.h"


extern "C" SEXP geoarrow_c_cast(SEXP array_from_sexp, SEXP array_to_sexp) {
    CPP_START

    struct ArrowSchema* schema_from = schema_from_xptr(
        VECTOR_ELT(array_from_sexp, 0),
        "array$schema");
    struct ArrowArray* array_data_from = array_data_from_xptr(
        VECTOR_ELT(array_from_sexp, 1),
        "array$array_data");

    struct ArrowSchema* schema_to = schema_from_xptr(
        VECTOR_ELT(array_to_sexp, 0),
        "array$schema");
    struct ArrowArray* array_data_to = reinterpret_cast<struct ArrowArray*>(
        R_ExternalPtrAddr(VECTOR_ELT(array_to_sexp, 1)));

    // Get the ArrayView to read array_from
    geoarrow::ArrayView* view = geoarrow::create_view(schema_from);
    SEXP view_xptr = PROTECT(R_MakeExternalPtr(view, R_NilValue, R_NilValue));
    R_RegisterCFinalizer(view_xptr, &delete_array_view_xptr);

    // Get the builder to build array_to
    geoarrow::GeoArrayBuilder* builder = geoarrow::create_builder(schema_to, 1024);
    SEXP builder_xptr = PROTECT(R_MakeExternalPtr(builder, array_to_sexp, R_NilValue));
    R_RegisterCFinalizer(builder_xptr, &delete_array_builder_xptr);

    // Do the cast operation
    view->read_meta(builder);
    view->set_array(array_data_from);
    view->read_features(builder);

    // The actual schema may be different than the requested schema
    if (schema_to->release != nullptr) {
        schema_to->release(schema_to);
    }

    // Transfer ownership of the built array_data and schema to array_to
    builder->release(array_data_to, schema_to);

    // The pointers pointed to by array_to have been modified in place,
    // but return it anyway in case we do something else in the future
    UNPROTECT(2);
    return array_to_sexp;
    CPP_END
}
