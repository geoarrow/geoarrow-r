
# for testing
geoarrow_create_wkt <- function(x, ...) {
  geoarrow_create(wk::new_wk_wkt(x), schema = geoarrow_schema_wkt(...))
}

handle_geoarrow_wkt <- function(array, handler) {
  .Call(geoarrow_c_handle_wkt, array, handler)
}

handle_geoarrow_wkt_stream <- function(array_stream, handler,
                                       schema = carrow::carrow_array_stream_get_schema(array_stream),
                                       n_features = NA_integer_) {
  stop("streaming not implemented for WKT", call. = FALSE)
}
