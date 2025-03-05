
# exported in zzz.R
st_as_sfc.geoarrow_vctr <- function(x, ..., promote_multi = FALSE) {
  sfc <- wk::wk_handle(x, wk::sfc_writer(promote_multi))
  wk::wk_set_crs(sfc, wk::wk_crs(x))
}

st_as_sfc.ChunkedArray <- function(x, ..., promote_multi = FALSE) {
  vctr <- as_geoarrow_vctr(x)
  st_as_sfc.geoarrow_vctr(vctr, ..., promote_multi = promote_multi)
}

st_as_sfc.Array <- function(x, ..., promote_multi = FALSE) {
  st_as_sfc.ChunkedArray(x, ..., promote_multi = promote_multi)
}

st_as_sfc.nanoarrow_array <- function(x, ..., promote_multi = FALSE) {
  st_as_sfc.ChunkedArray(x, ..., promote_multi = promote_multi)
}

# nolint start: object_length_linter.
st_as_sfc.nanoarrow_array_stream <- function(x, ..., promote_multi = FALSE) {
  st_as_sfc.ChunkedArray(x, ..., promote_multi = promote_multi)
}
# nolint end

st_as_sf.ArrowTabular <- function(x, ..., promote_multi = FALSE) {
  # Some Arrow as.data.frame() methods still return tibbles
  df <- as.data.frame(as.data.frame(x))
  is_geom <- vapply(df, inherits, logical(1), "geoarrow_vctr")
  df[is_geom] <- lapply(df[is_geom], sf::st_as_sfc, promote_multi = promote_multi)
  sf::st_as_sf(df, ...)
}

st_as_sf.Dataset <- function(x, ..., promote_multi = FALSE) {
  st_as_sf.ArrowTabular(x, ..., promote_multi = promote_multi)
}

st_as_sf.Scanner <- function(x, ..., promote_multi = FALSE) {
  sf::st_as_sf(x$ToTable(), promote_multi = promote_multi)
}

st_as_sf.RecordBatchReader <- function(x, ..., promote_multi = FALSE) {
  st_as_sf.ArrowTabular(x, ..., promote_multi = promote_multi)
}

st_as_sf.arrow_dplyr_query <- function(x, ..., promote_multi = FALSE) {
  st_as_sf.ArrowTabular(x, ..., promote_multi = promote_multi)
}

# nolint start: object_length_linter.
st_as_sf.nanoarrow_array_stream <- function(x, ..., promote_multi = FALSE) {
  st_as_sf.ArrowTabular(x, ..., promote_multi = promote_multi)
}
# nolint end

st_as_sf.nanoarrow_array <- function(x, ..., promote_multi = FALSE) {
  st_as_sf.ArrowTabular(x, ..., promote_multi = promote_multi)
}

as_arrow_array.sfc <- function(x, ..., type = NULL) {
  if (!is.null(type)) {
    type <- nanoarrow::as_nanoarrow_schema(type)
  }

  arrow::as_arrow_array(as_geoarrow_vctr(x, schema = type))
}

as_chunked_array.sfc <- function(x, ..., type = NULL) {
  if (!is.null(type)) {
    type <- nanoarrow::as_nanoarrow_schema(type)
  }

  arrow::as_chunked_array(as_geoarrow_vctr(x, schema = type))
}

infer_type.sfc <- function(x, ...) {
  arrow::as_data_type(nanoarrow::infer_nanoarrow_schema(x))
}

#' @export
convert_array.sfc <- function(array, to, ..., sfc_promote_multi = FALSE) {
  vctr <- as_geoarrow_vctr(array)
  st_as_sfc.geoarrow_vctr(vctr, promote_multi = sfc_promote_multi)
}

#' @importFrom nanoarrow infer_nanoarrow_schema
#' @export
infer_nanoarrow_schema.sfc <- function(x, ...) {
  infer_geoarrow_schema(x)
}

#' @export
as_geoarrow_array.sfc <- function(x, ..., schema = NULL) {
  # Let the default method handle custom output schemas
  if (!is.null(schema)) {
    return(NextMethod())
  }

  meta <- wk::wk_vector_meta(x)

  # Let the default method handle M values (the optimized path doesn't
  # handle mixed XYZ/XYZM/XYM but can deal with mixed XY and XYZ)
  if (meta$has_m) {
    return(NextMethod())
  }

  if (meta$geometry_type %in% 1:6) {
    schema <- infer_geoarrow_schema(x)
    array <- nanoarrow::nanoarrow_allocate_array()
    .Call(geoarrow_c_as_nanoarrow_array_sfc, x, schema, array)
    nanoarrow::nanoarrow_array_set_schema(array, schema)
    array
  } else {
    NextMethod()
  }
}

#' @importFrom nanoarrow as_nanoarrow_array
#' @export
as_nanoarrow_array.sfc <- function(x, ..., schema = NULL) {
  as_geoarrow_array(x, ..., schema = schema)
}

#' @importFrom nanoarrow as_nanoarrow_array_stream
#' @export
as_nanoarrow_array_stream.sfc <- function(x, ..., schema = NULL) {
  as_geoarrow_array_stream(x, ..., schema = schema)
}
