
#ifndef UTIL_H_INCLUDED
#define UTIL_H_INCLUDED

#include <R.h>
#include <Rinternals.h>

#ifdef __cplusplus
extern "C" {
#endif

void geoarrow_finalize_array_data(SEXP array_data_xptr);

#ifdef __cplusplus
}
#endif

#endif
