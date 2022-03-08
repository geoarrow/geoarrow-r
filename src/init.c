#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

SEXP geoarrow_c_handle_point(SEXP data, SEXP handler_xptr);

static const R_CallMethodDef CallEntries[] = {
    {"geoarrow_c_handle_point", (DL_FUNC) &geoarrow_c_handle_point, 2},
    {NULL, NULL, 0}
};

void R_init_geoarrow(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
