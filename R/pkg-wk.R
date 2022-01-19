
#' @importFrom wk wk_crs
#' @export
wk_crs.sparrow_array <- function(x) {
  wk_crs_sparrow_schema(x$schema)
}

wk_crs_sparrow_schema <- function(schema) {
  geo_metadata <- geoarrow_metadata(schema)
  if (!is.null(geo_metadata$crs)) {
    geo_metadata$crs
  } else {
    for (child in schema$children) {
      geo_metadata <- geoarrow_metadata(child)
      if (!is.null(geo_metadata$crs)) {
        return(geo_metadata$crs)
      } else {
        return(wk_crs_sparrow_schema(child))
      }
    }
  }
}
