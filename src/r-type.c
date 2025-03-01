#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "geoarrow/geoarrow.h"

SEXP geoarrow_c_make_type(SEXP geometry_type_sexp, SEXP dimensions_sexp,
                          SEXP coord_type_sexp) {
  enum GeoArrowType out =
      GeoArrowMakeType((enum GeoArrowGeometryType)INTEGER(geometry_type_sexp)[0],
                       (enum GeoArrowDimensions)INTEGER(dimensions_sexp)[0],
                       (enum GeoArrowCoordType)INTEGER(coord_type_sexp)[0]);

  return Rf_ScalarInteger(out);
}

SEXP geoarrow_c_schema_init_extension(SEXP schema_xptr, SEXP type_sexp) {
  struct ArrowSchema* schema = (struct ArrowSchema*)R_ExternalPtrAddr(schema_xptr);
  enum GeoArrowType type_id = (enum GeoArrowType)INTEGER(type_sexp)[0];

  int result = GeoArrowSchemaInitExtension(schema, type_id);
  if (result != GEOARROW_OK) {
    Rf_error("[GeoArrowSchemaInitExtension][%d] type_id not valid", result);
  }

  return R_NilValue;
}

SEXP geoarrow_c_schema_parse(SEXP schema_xptr, SEXP extension_name_sexp) {
  struct ArrowSchema* schema = (struct ArrowSchema*)R_ExternalPtrAddr(schema_xptr);

  struct GeoArrowSchemaView schema_view;
  struct GeoArrowError error;
  error.message[0] = '\0';

  int result;
  if (extension_name_sexp == R_NilValue) {
    result = GeoArrowSchemaViewInit(&schema_view, schema, &error);
    if (result != GEOARROW_OK) {
      Rf_error("GeoArrowSchemaViewInit() failed: %s", error.message);
    }
  } else {
    SEXP extension_name_elt = STRING_ELT(extension_name_sexp, 0);
    if (extension_name_elt == NA_STRING) {
      Rf_error("extension_name must not be NA");
    }

    const char* extension_name = Rf_translateCharUTF8(extension_name_elt);
    struct GeoArrowStringView extension_name_view;
    extension_name_view.data = extension_name;
    extension_name_view.size_bytes = strlen(extension_name);
    result = GeoArrowSchemaViewInitFromStorage(&schema_view, schema, extension_name_view,
                                               &error);
    if (result != GEOARROW_OK) {
      Rf_error("GeoArrowSchemaViewInitFromStorage() failed: %s", error.message);
    }
  }

  struct GeoArrowMetadataView metadata_view;
  result =
      GeoArrowMetadataViewInit(&metadata_view, schema_view.extension_metadata, &error);
  if (result != GEOARROW_OK) {
    Rf_error("GeoArrowMetadataViewInit() failed: %s", error.message);
  }

  const char* names[] = {"id",         "geometry_type",  "dimensions",
                         "coord_type", "extension_name", "crs_type",
                         "crs",        "edge_type",      ""};
  SEXP parsed_sexp = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(parsed_sexp, 0, Rf_ScalarInteger(schema_view.type));
  SET_VECTOR_ELT(parsed_sexp, 1, Rf_ScalarInteger(schema_view.geometry_type));
  SET_VECTOR_ELT(parsed_sexp, 2, Rf_ScalarInteger(schema_view.dimensions));
  SET_VECTOR_ELT(parsed_sexp, 3, Rf_ScalarInteger(schema_view.coord_type));

  SEXP extension_name_elt_out = PROTECT(Rf_mkCharLenCE(
      schema_view.extension_name.data, schema_view.extension_name.size_bytes, CE_UTF8));
  SET_VECTOR_ELT(parsed_sexp, 4, Rf_ScalarString(extension_name_elt_out));
  UNPROTECT(1);

  SET_VECTOR_ELT(parsed_sexp, 5, Rf_ScalarInteger(metadata_view.crs_type));

  int64_t crs_size_bytes = GeoArrowUnescapeCrs(metadata_view.crs, NULL, 0);
  SEXP crs_shelter_sexp = PROTECT(Rf_allocVector(RAWSXP, crs_size_bytes));
  GeoArrowUnescapeCrs(metadata_view.crs, (char*)RAW(crs_shelter_sexp), crs_size_bytes);
  SEXP crs_elt_out =
      PROTECT(Rf_mkCharLenCE((char*)RAW(crs_shelter_sexp), crs_size_bytes, CE_UTF8));
  SET_VECTOR_ELT(parsed_sexp, 6, Rf_ScalarString(crs_elt_out));
  UNPROTECT(2);

  SET_VECTOR_ELT(parsed_sexp, 7, Rf_ScalarInteger(metadata_view.edge_type));

  UNPROTECT(1);
  return parsed_sexp;
}
