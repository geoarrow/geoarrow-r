
#' Create GeoArrow vectors
#'
#' @param x An object to convert to a GeoArrow representation.
#' @inheritParams geoarrow_create_narrow
#'
#' @return An object of class 'narrow_vctr_geoarrow_EXTENSION' and
#'    [narrow_vctr()].
#' @export
#'
as_geoarrow_vctr <- function(x, ..., schema = geoarrow_schema_default(x),
                             strict = FALSE) {
  array <- geoarrow_create_narrow(x, schema = schema, strict = strict)
  narrow::narrow_vctr(array)
}

# exported dynamically in zzz.R
vctr_proxy <- function(x, ...) {
  x
}

vctr_restore <- function(x, to, ...) {
  narrow::new_narrow_vctr(x, attr(to, "array", exact = TRUE))[]
}

# maybe a better way to do this using an abstract class

format_geoarrow_vctr <- function(x, ...) {
  wk::wk_format(narrow::as_narrow_array(x))
}

as.character_geoarrow_vctr <- function(x, ...) {
  format_geoarrow_vctr(x)
}

#' @export
format.narrow_vctr_geoarrow <- function(x, ...) {
  wk::wk_format(narrow::as_narrow_array(x))
}

#' @export
as.character.narrow_vctr_geoarrow <- function(x, ...) {
  wk::wk_format(narrow::as_narrow_array(x))
}
