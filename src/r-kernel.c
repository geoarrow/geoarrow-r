
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "geoarrow.h"

static void finalize_kernel_xptr(SEXP xptr) {
  struct GeoArrowKernel* kernel = (struct GeoArrowKernel*)R_ExternalPtrAddr(xptr);
  if (kernel->release != NULL) {
    kernel->release(kernel);
  }

  free(kernel);
}

SEXP geoarrow_c_kernel(SEXP kernel_name_sexp, SEXP arg_types_sexp, SEXP options_sexp,
                       SEXP schema_out_xptr) {
  SEXP kernel_name_elt = STRING_ELT(kernel_name_sexp, 0);
  const char* kernel_name = Rf_translateCharUTF8(kernel_name_elt);

  const char* options = (char*)RAW(options_sexp);

  struct ArrowSchema* schema_out =
      (struct ArrowSchema*)R_ExternalPtrAddr(schema_out_xptr);

  struct GeoArrowError error;
  error.message[0] = '\0';

  // All kernels currently have just one argument
  struct ArrowSchema* args[1];
  int n_args = Rf_length(arg_types_sexp);
  if (n_args != 1) {
    Rf_error("Incorrect number of arguments");
  }

  for (int i = 0; i < n_args; i++) {
    args[i] = (struct ArrowSchema*)R_ExternalPtrAddr(VECTOR_ELT(arg_types_sexp, i));
  }

  struct GeoArrowKernel* kernel =
      (struct GeoArrowKernel*)malloc(sizeof(struct GeoArrowKernel));
  if (kernel == NULL) {
    Rf_error("Failed to allocate struct GeoArrowKernel");
  }

  kernel->release = NULL;
  SEXP kernel_xptr = PROTECT(R_MakeExternalPtr(kernel, R_NilValue, R_NilValue));
  R_RegisterCFinalizer(kernel_xptr, &finalize_kernel_xptr);

  int result = GeoArrowKernelInit(kernel, kernel_name, NULL);
  if (result != GEOARROW_OK) {
    Rf_error("GeoArrowKernelInit() failed");
  }

  result = kernel->start(kernel, args[0], options, schema_out, &error);
  if (result != GEOARROW_OK) {
    Rf_error("kernel->start() failed [%d]: %s", result, error.message);
  }

  UNPROTECT(1);
  return kernel_xptr;
}

SEXP geoarrow_c_kernel_push(SEXP kernel_xptr, SEXP args_sexp, SEXP array_out_xptr) {
  struct GeoArrowKernel* kernel = (struct GeoArrowKernel*)R_ExternalPtrAddr(kernel_xptr);

  struct ArrowArray* array_out;
  if (array_out_xptr == R_NilValue) {
    array_out = NULL;
  } else {
    array_out = (struct ArrowArray*)R_ExternalPtrAddr(array_out_xptr);
  }

  // All kernels currently have just one argument
  struct ArrowArray* args[1];
  memset(args, 0, sizeof(args));

  int n_args = Rf_length(args_sexp);
  if (n_args != 1) {
    Rf_error("Kernels must be passed one argument");
  }

  for (int i = 0; i < n_args; i++) {
    args[i] = (struct ArrowArray*)R_ExternalPtrAddr(VECTOR_ELT(args_sexp, i));
  }

  struct GeoArrowError error;
  error.message[0] = '\0';

  int result = kernel->push_batch(kernel, args[0], array_out, &error);
  if (result != GEOARROW_OK) {
    Rf_error("kernel->push_batch() failed [%d]: %s", result, error.message);
  }

  return R_NilValue;
}

SEXP geoarrow_c_kernel_finish(SEXP kernel_xptr, SEXP array_out_xptr) {
  struct GeoArrowKernel* kernel = (struct GeoArrowKernel*)R_ExternalPtrAddr(kernel_xptr);

  struct ArrowArray* array_out;
  if (array_out_xptr == R_NilValue) {
    array_out = NULL;
  } else {
    array_out = (struct ArrowArray*)R_ExternalPtrAddr(array_out_xptr);
  }

  struct GeoArrowError error;
  error.message[0] = '\0';

  int result = kernel->finish(kernel, array_out, &error);
  if (result != GEOARROW_OK) {
    Rf_error("kernel->finish() failed [%d]: %s", result, error.message);
  }

  return R_NilValue;
}
