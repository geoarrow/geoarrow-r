
#' GeoArrow encoded arrays as R vectors
#'
#' @param x An object that works with [as_geoarrow_array_stream()]. Most
#'   spatial objects in R already work with this method.
#' @param ... Passed to [as_geoarrow_array_stream()]
#' @param schema An optional `schema` (e.g., [na_extension_geoarrow()]).
#'
#' @return A vctr of class 'geoarrow_vctr'
#' @export
#'
#' @examples
#' as_geoarrow_vctr("POINT (0 1)")
#'
as_geoarrow_vctr <- function(x, ..., schema = NULL) {
  if (inherits(x, "geoarrow_vctr") && is.null(schema)) {
    return(x)
  }

  stream <- as_geoarrow_array_stream(x, ..., schema = schema)
  nanoarrow::as_nanoarrow_vctr(stream, subclass = "geoarrow_vctr")
}

#' @export
format.geoarrow_vctr <- function(x, ..., width = NULL, digits = NULL) {
  if (is.null(width)) {
    width <- getOption("width", 100L)
  }

  width <- max(width, 20)

  if (is.null(digits)) {
    digits <- getOption("digits", 7L)
  }

  digits <- max(digits, 0)

  formatted_array <- geoarrow_kernel_call_scalar(
    "format_wkt",
    x,
    options = c(
      max_element_size_bytes = width - 10L,
      precision = digits
    ),
    n = length(attr(x, "chunks", exact = TRUE))
  )

  formatted_chr <- nanoarrow::convert_array_stream(
    formatted_array,
    character(),
    size = length(x)
  )

  sprintf("<%s>", formatted_chr)
}

# Because RStudio's viewer uses this, we want to use the potentially abbreviated
# WKT from the format method
#' @export
as.character.geoarrow_vctr <- function(x, ...) {
  format(x, ...)
}

#' @export
as_geoarrow_array_stream.geoarrow_vctr <- function(x, ..., schema = NULL) {
  stream <- nanoarrow::as_nanoarrow_array_stream(x)
  as_geoarrow_array_stream(stream, schema = schema)
}
