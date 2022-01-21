
#' @importFrom wk wk_crs
#' @export
wk_crs.narrow_array <- function(x) {
  recursive_extract_narrow_schema(x$schema, "crs")
}

#' @importFrom wk wk_is_geodesic
#' @export
wk_is_geodesic.narrow_array <- function(x) {
  identical(
    recursive_extract_narrow_schema(x$schema, "geodesic"),
    "true"
  )
}

recursive_extract_narrow_schema <- function(schema, key) {
  geo_metadata <- geoarrow_metadata(schema)
  if (!is.null(geo_metadata[[key]])) {
    geo_metadata[[key]]
  } else {
    for (child in schema$children) {
      geo_metadata <- geoarrow_metadata(child)
      if (!is.null(geo_metadata[[key]])) {
        return(geo_metadata[[key]])
      } else {
        return(recursive_extract_narrow_schema(child))
      }
    }
  }
}
