#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

SEXP geoarrow_c_is_slice(SEXP values_sexp) {
    if (TYPEOF(values_sexp) == INTSXP) {
        int n = Rf_length(values_sexp);

        if (n == 1) {
            return Rf_ScalarLogical(1);
        } else if (n == 0) {
            return Rf_ScalarLogical(0);
        }

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

SEXP geoarrow_c_is_identity_slice(SEXP values_sexp, SEXP total_len) {
    if (TYPEOF(values_sexp) != INTSXP ||
        Rf_length(values_sexp) == 0 ||
        TYPEOF(total_len) != INTSXP ||
        Rf_length(total_len) != 1) {
        return Rf_ScalarLogical(0);
    }

    SEXP is_slice_sexp = PROTECT(geoarrow_c_is_slice(values_sexp));
    int is_slice = LOGICAL(is_slice_sexp)[0];
    UNPROTECT(1);

    int result = is_slice &&
        INTEGER_ELT(values_sexp, 0) == 1 &&
        INTEGER_ELT(values_sexp, Rf_length(values_sexp) - 1) == INTEGER_ELT(total_len, 0);
    return Rf_ScalarLogical(result);
}


