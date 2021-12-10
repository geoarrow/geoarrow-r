
#' Handle Arrow arrays
#'
#' @inheritParams wk::wk_handle
#'
#' @return The result of `handler`
#' @export
#' @importFrom wk wk_handle
#'
wk_handle.carrow_array <- function(handleable, handler, ...) {
  handler <- wk::as_wk_handler(handler)
  metadata <- handleable$schema$metadata
  extension <- scalar_chr(metadata[["ARROW:extension:name"]])
  geo_metadata <- geoarrow_metadata(handleable$schema)

  switch(
    extension,
    "geoarrow.wkb" = handle_geoarrow_wkb(handleable, handler),
    "geoarrow.wkt" = {
      chr <- carrow::from_carrow_array(handleable, character())
      wk::wk_handle(wk::new_wk_wkt(chr, crs = geo_metadata$crs), handler)
    },
    "geoarrow.geojson" = {
      assert_geos_with_geojson()
      chr <- carrow::from_carrow_array(handleable, character())
      wk::wk_handle(geos::geos_read_geojson(chr, crs = geo_metadata$crs), handler)
    },
    "geoarrow.point" =,
    "geoarrow.linestring" =,
    "geoarrow.multi" =,
    stop(sprintf("Unsupported extension type '%s'", extension), call. = FALSE)
  )
}
