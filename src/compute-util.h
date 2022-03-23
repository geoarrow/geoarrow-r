
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "geoarrow.h"
#include "narrow.h"

#include "util.h"

static inline SEXP compute_options_from_sexp(SEXP options_sexp) {
    CPP_START

    if (TYPEOF(options_sexp) != VECSXP) {
        Rf_error("`options` must be a list()");
    }

    geoarrow::ComputeOptions* options = new geoarrow::ComputeOptions();
    SEXP options_xptr = PROTECT(R_MakeExternalPtr(options, R_NilValue, R_NilValue));
    R_RegisterCFinalizer(options_xptr, &delete_compute_options_xptr);

    if (Rf_length(options_sexp) == 0) {
        UNPROTECT(1);
        return options_xptr;
    }

    SEXP names_sexp = Rf_getAttrib(options_sexp, R_NamesSymbol);
    if (names_sexp == R_NilValue) {
        Rf_error("`names(options)` is NULL");
    }

    for (int i = 0; i < Rf_length(options_sexp); i++) {
        SEXP name_sexp = STRING_ELT(names_sexp, i);
        if (name_sexp == NA_STRING) {
            Rf_error("`names(options)[%d]` is NA", i + 1);
        }

        const char* name = Rf_translateCharUTF8(name_sexp);

        SEXP value = VECTOR_ELT(options_sexp, i);
        if (Rf_inherits(value, "narrow_schema")) {
            options->set_schema(name, schema_from_xptr(value, "options[]"));
        } else if (TYPEOF(value) == LGLSXP && Rf_length(value) == 1) {
            options->set_bool(name, LOGICAL(value)[0] != 0);
        } else {
            Rf_error("Can't convert `options[\"%s\"]` to ComputeOptions type");
        }
    }

    UNPROTECT(1);
    return options_xptr;

    CPP_END
}
