
#' Handle Arrow arrays
#'
#' @inheritParams wk::wk_handle
#' @param geoarrow_schema Override the `schema` of the array stream
#'   (e.g., to provide geo metadata).
#' @param geoarrow_n_features Manually specify the number of features
#'   when reading a stream if this value is known (or `NA_integer`
#'   if it is not).
#'
#' @return The result of `handler`
#' @export
#' @importFrom wk wk_handle
#'
wk_handle.sparrow_array <- function(handleable, handler, ...) {
  handler <- wk::as_wk_handler(handler)
  metadata <- handleable$schema$metadata
  extension <- scalar_chr(metadata[["ARROW:extension:name"]])
  geo_metadata <- geoarrow_metadata(handleable$schema)

  switch(
    extension,
    "geoarrow.wkb" = handle_geoarrow_wkb(handleable, handler),
    "geoarrow.wkt" = handle_geoarrow_wkt(handleable, handler),
    "geoarrow.geojson" = {
      assert_geos_with_geojson()
      chr <- sparrow::from_sparrow_array(handleable, character())
      wk::wk_handle(geos::geos_read_geojson(chr, crs = geo_metadata$crs), handler)
    },
    "geoarrow.point" = ,
    "geoarrow.linestring" = ,
    "geoarrow.polygon" = ,
    "geoarrow.multi" = handle_geoarrow_point(handleable, handler),
    stop(sprintf("Unsupported extension type '%s'", extension), call. = FALSE)
  )
}

#' @export
#' @rdname wk_handle.sparrow_array
wk_handle.sparrow_array_stream <- function(handleable, handler, ...,
                                          geoarrow_schema = sparrow::sparrow_array_stream_get_schema(handleable),
                                          geoarrow_n_features = NA_integer_) {
  handler <- wk::as_wk_handler(handler)
  metadata <- geoarrow_schema$metadata
  extension <- scalar_chr(metadata[["ARROW:extension:name"]])
  geo_metadata <- geoarrow_metadata(geoarrow_schema)

  switch(
    extension,
    "geoarrow.wkb" = handle_geoarrow_wkb_stream(handleable, handler),
    "geoarrow.wkt" = handle_geoarrow_wkt_stream(handleable, handler),
    "geoarrow.point" = ,
    "geoarrow.linestring" = ,
    "geoarrow.polygon" = ,
    "geoarrow.multi" = handle_geoarrow_point_stream(handleable, handler, geoarrow_schema, geoarrow_n_features),
    stop(sprintf("Unsupported extension type '%s'", extension), call. = FALSE)
  )
}
