
#ifndef UTIL_H_INCLUDED
#define UTIL_H_INCLUDED

#include <R.h>
#include <Rinternals.h>

#define CPP_START                         \
    char cpp_exception_error[8096];       \
    memset(cpp_exception_error, 0, 8096); \
    try {

#define CPP_END                                           \
    } catch (std::exception& e) {                         \
        strncpy(cpp_exception_error, e.what(), 8096 - 1); \
    }                                                     \
    Rf_error("%s", cpp_exception_error);                  \
    return R_NilValue;

void geoarrow_finalize_array_data(SEXP array_data_xptr);
void delete_array_view_xptr(SEXP array_view_xptr);
void delete_array_builder_xptr(SEXP array_builder_xptr);
void delete_compute_options_xptr(SEXP compute_options_xptr);

#endif
