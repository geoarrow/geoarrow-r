#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

SEXP geoarrow_c_is_slice(SEXP values_sexp) {
    int n = Rf_length(values_sexp);
    if (n == 1) {
        return Rf_ScalarLogical(1);
    } else if (n == 0) {
        return Rf_ScalarLogical(0);
    }

    if (TYPEOF(values_sexp) == INTSXP) {
        int buf[1024];
        INTEGER_GET_REGION(values_sexp, 0, 1024, buf);

        int last_value = buf[0];
        int this_value = 0;

        for (int i = 1; i < n; i++) {
            if (i % 1024 == 0) {
                INTEGER_GET_REGION(values_sexp, i, 1024, buf);
            }

            this_value = buf[i % 1024];
            if ((this_value - last_value) != 1) {
                return Rf_ScalarLogical(0);
            }

            last_value = this_value;
        }

        return Rf_ScalarLogical(1);
    } else {
        return Rf_ScalarLogical(0);
    }
}
