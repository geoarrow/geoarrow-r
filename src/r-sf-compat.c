#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "geoarrow/geoarrow.h"

// From sf/src/wkb.cpp to apply the precision from attr(sfc, "precision")
static double make_precise(double d, double precision) {
  if (precision == 0.0) return d;
  if (precision < 0.0) {  // round to float, 4-byte precision
    float f = d;
    return (double)f;
  }
  return round(d * precision) / precision;
}

static void make_buffer_precise(double* ptr, int size_elements, double precision) {
  if (precision == 0) {
    return;
  }

  for (int i = 0; i < size_elements; i++) {
    ptr[i] = make_precise(ptr[i], precision);
  }
}

static inline int builder_append_sfg(SEXP item, struct GeoArrowBuilder* builder,
                                     int level, int32_t* current_offsets,
                                     double precision) {
  switch (TYPEOF(item)) {
    // Level of nesting
    case VECSXP: {
      if (level >= builder->view.n_offsets) {
        Rf_error("Unexpected level of nesting whilst buliding ArrowArray from sfc");
      }

      int32_t n = Rf_length(item);
      current_offsets[level] += n;
      GEOARROW_RETURN_NOT_OK(
          GeoArrowBuilderOffsetAppend(builder, level, current_offsets + level, 1));
      for (int32_t i = 0; i < n; i++) {
        builder_append_sfg(VECTOR_ELT(item, i), builder, level + 1, current_offsets,
                           precision);
      }
      break;
    }

    // Matrix containing a coordinate sequence
    case REALSXP: {
      int32_t n = Rf_nrows(item);
      current_offsets[level] += n;
      GEOARROW_RETURN_NOT_OK(
          GeoArrowBuilderOffsetAppend(builder, level, current_offsets + level, 1));

      if (n == 0) {
        return GEOARROW_OK;
      }

      int n_col = Rf_ncols(item);
      double* coords = REAL(item);
      struct GeoArrowBufferView view;
      view.data = (uint8_t*)coords;
      view.size_bytes = n * sizeof(double);

      int first_coord_buffer = 1 + builder->view.n_offsets;
      for (int i = 0; i < n_col; i++) {
        // Omit dimensions in sfc but not in builder
        if (i >= builder->view.coords.n_values) {
          break;
        }

        // Copy the buffer
        GEOARROW_RETURN_NOT_OK(
            GeoArrowBuilderAppendBuffer(builder, first_coord_buffer + i, view));

        // Apply precision from sfc
        double* ordinates =
            (double*)(builder->view.buffers[first_coord_buffer + i].data.as_uint8 +
                      builder->view.buffers[first_coord_buffer + i].size_bytes);
        ordinates -= n_col;
        make_buffer_precise(ordinates, n_col, precision);

        view.data += view.size_bytes;
      }

      // Fill dimensions in builder but not in sfc with nan
      for (int i = n_col; i < builder->view.coords.n_values; i++) {
        double nan_dbl = NAN;
        view.data = (uint8_t*)&nan_dbl;
        view.size_bytes = sizeof(double);
        GEOARROW_RETURN_NOT_OK(GeoArrowBuilderReserveBuffer(
            builder, first_coord_buffer + i, n * sizeof(double)));
        for (int j = 0; j < n; j++) {
          GeoArrowBuilderAppendBufferUnsafe(builder, first_coord_buffer + i, view);
        }
      }
      break;
    }

    default:
      Rf_error("Unexpected element whilst building ArrowArray from sfc");
  }

  return GEOARROW_OK;
}

static inline int builder_append_sfc_point(SEXP sfc, struct GeoArrowBuilder* builder,
                                           double precision) {
  R_xlen_t n = Rf_xlength(sfc);

  for (int i = 0; i < builder->view.coords.n_values; i++) {
    GEOARROW_RETURN_NOT_OK(GeoArrowBuilderCoordsReserve(builder, n));
  }

  SEXP item_sexp;
  double* item;
  int coord_size;
  for (R_xlen_t i = 0; i < n; i++) {
    item_sexp = VECTOR_ELT(sfc, i);
    item = REAL(item_sexp);
    coord_size = Rf_length(item_sexp);
    for (int j = 0; j < coord_size; j++) {
      // Omit dimensions in sfc but not in builder
      if (j >= builder->view.coords.n_values) {
        break;
      }

      builder->view.coords.values[j][i] = make_precise(item[j], precision);
    }

    // Fill dimensions in builder but not in sfc with nan
    for (int j = coord_size; j < builder->view.coords.n_values; j++) {
      builder->view.coords.values[j][i] = NAN;
    }
  }

  builder->view.coords.size_coords = n;
  builder->view.length = n;
  return GEOARROW_OK;
}

static int builder_append_sfc(SEXP sfc, struct GeoArrowBuilder* builder,
                              double precision) {
  if (Rf_inherits(sfc, "sfc_POINT")) {
    return builder_append_sfc_point(sfc, builder, precision);
  }

  R_xlen_t n = Rf_xlength(sfc);

  // Append initial 0 to the offset buffers and reserve memory for their minimum
  // likely size (might be inaccurate for sfcs with a lot of empties).
  int32_t zero = 0;
  GEOARROW_RETURN_NOT_OK(GeoArrowBuilderOffsetReserve(builder, 0, n + 1));
  GeoArrowBuilderOffsetAppendUnsafe(builder, 0, &zero, 1);

  for (int i = 1; i < builder->view.n_offsets; i++) {
    GEOARROW_RETURN_NOT_OK(GeoArrowBuilderOffsetReserve(builder, i, (n + 1) * 1.5));
    GeoArrowBuilderOffsetAppendUnsafe(builder, i, &zero, 1);
  }

  // Keep track of current last value
  int32_t current_offsets[] = {0, 0, 0};

  // Append elements
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP item = VECTOR_ELT(sfc, i);
    GEOARROW_RETURN_NOT_OK(
        builder_append_sfg(item, builder, 0, current_offsets, precision));
  }

  builder->view.length = n;
  return GEOARROW_OK;
}

static void finalize_builder_xptr(SEXP builder_xptr) {
  struct GeoArrowBuilder* builder =
      (struct GeoArrowBuilder*)R_ExternalPtrAddr(builder_xptr);
  if (builder != NULL && builder->private_data != NULL) {
    GeoArrowBuilderReset(builder);
  }

  if (builder != NULL) {
    free(builder);
  }
}

SEXP geoarrow_c_as_nanoarrow_array_sfc(SEXP sfc, SEXP schema_xptr, SEXP array_xptr) {
  struct ArrowSchema* schema = (struct ArrowSchema*)R_ExternalPtrAddr(schema_xptr);
  struct ArrowArray* array = (struct ArrowArray*)R_ExternalPtrAddr(array_xptr);

  // Use external pointer finalizer to ensure builder is cleaned up
  struct GeoArrowBuilder* builder =
      (struct GeoArrowBuilder*)malloc(sizeof(struct GeoArrowBuilder));
  if (builder == NULL) {
    Rf_error("Failed to allocate for GeoArrowBuilder");
  }
  builder->private_data = NULL;
  SEXP builder_xptr = PROTECT(R_MakeExternalPtr(builder, R_NilValue, R_NilValue));
  R_RegisterCFinalizer(builder_xptr, &finalize_builder_xptr);

  // Get the precision from the sf object so we can apply it if needed
  double precision = 0;
  SEXP precision_sym = PROTECT(Rf_install("precision"));
  SEXP precision_sexp = Rf_getAttrib(sfc, precision_sym);
  UNPROTECT(1);
  if (precision_sexp != R_NilValue && Rf_length(precision_sexp) == 1) {
    switch (TYPEOF(precision_sexp)) {
      case INTSXP:
        precision = (double)INTEGER(precision_sexp)[0];
        break;
      case REALSXP:
        precision = REAL(precision_sexp)[0];
        break;
      default:
        break;
    }
  }

  struct GeoArrowError error;
  error.message[0] = '\0';

  // Initialize the builder
  int result = GeoArrowBuilderInitFromSchema(builder, schema, &error);
  if (result != GEOARROW_OK) {
    Rf_error("GeoArrowBuilderInitFromSchema() failed: %s", error.message);
  }

  // Build the offset buffers from the various layers of nesting
  result = builder_append_sfc(sfc, builder, precision);
  if (result != GEOARROW_OK) {
    Rf_error("builder_append_sfc() failed to allocate memory for offset buffers");
  }

  // Build result
  result = GeoArrowBuilderFinish(builder, array, &error);
  if (result != GEOARROW_OK) {
    Rf_error("GeoArrowBuilderFinish() failed: %s", error.message);
  }

  UNPROTECT(1);
  return R_NilValue;
}
