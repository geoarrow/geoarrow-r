
#' Handler/writer interface for GeoArrow arrays
#'
#' @param x An object implementing [as_geoarrow_array_stream()]
#' @param handler A [wk handler][wk::wk_handle]
#' @param size The number of elements in the stream or NA if unknown
#' @param schema A [nanoarrow_schema][nanoarrow::as_nanoarrow_schema]
#'
#' @return
#'   - `geoarrow_handle()`: Returns the result of `handler`
#'   - `geoarrow_writer()`: Returns a [nanoarrow array][as_nanoarrow_array]
#' @export
#'
#' @examples
#' geoarrow_handle(wk::xy(1:3, 2:4), wk::wk_debug_filter())
#' wk::wk_handle(wk::xy(1:3, 2:4), geoarrow_writer(na_extension_wkt()))
#'
geoarrow_handle <- function(x, handler, size = NA_integer_) {
  stream <- as_geoarrow_array_stream(x)
  handler <- wk::as_wk_handler(handler)

  data <- list(
    stream,
    stream$get_schema(),
    nanoarrow::nanoarrow_allocate_array(),
    size
  )

  .Call(geoarrow_c_handle_stream, data, handler)
}

#' @rdname geoarrow_handle
#' @export
geoarrow_writer <- function(schema) {
  array_out <- nanoarrow::nanoarrow_allocate_array()
  schema <- nanoarrow::as_nanoarrow_schema(schema)
  nanoarrow::nanoarrow_array_set_schema(array_out, schema, validate = FALSE)
  wk::new_wk_handler(
    .Call(geoarrow_c_writer_new, schema, array_out),
    "geoarrow_writer"
  )
}
