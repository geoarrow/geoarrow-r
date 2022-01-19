
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "narrow.h"

void geoarrow_finalize_array_data(SEXP array_data_xptr) {
    struct ArrowArray* array_data = (struct ArrowArray*) R_ExternalPtrAddr(array_data_xptr);
    if (array_data != NULL && array_data->release != NULL) {
        array_data->release(array_data);
    }
}
