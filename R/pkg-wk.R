
#' @importFrom wk wk_crs
#' @export
wk_crs.carrow_array <- function(x) {
  metadata <- x$schema$metadata
  extension <- scalar_chr(metadata[["ARROW:extension:name"]])
  geo_metadata <- geoarrow_metadata(x$schema)
  geo_metadata$crs
}
