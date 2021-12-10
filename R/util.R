
scalar_chr <- function(x) {
  stopifnot(is.character(x), length(x) == 1, !is.na(x))
  x
}

scalar_lgl <- function(x) {
  stopifnot(is.logical(x), length(x) == 1, !is.na(x))
  x
}

assert_geos_with_geojson <- function() {
  if (!requireNamespace("geos", quietly = TRUE) || (geos::geos_version() < "3.10")) {
    # nocov start
    stop(
      "Package 'geos' with 'libgeos' >= 3.10 required to export handleable as GeoJSON",
      call. = FALSE
    )
    # nocov end
  }
}
