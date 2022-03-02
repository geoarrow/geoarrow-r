
handle_geoarrow_wkb <- function(array, handler) {
  handle_geoarrow_wkb_stream(
    narrow::as_narrow_array_stream(array),
    handler,
    array$schema,
    n_features = array$array_data$length
  )
}

handle_geoarrow_wkb_stream <- function(array_stream, handler,
                                       schema = narrow::narrow_array_stream_get_schema(array_stream),
                                       n_features = NA_integer_) {
  .Call(geoarrow_c_handle_point, list(array_stream, schema, n_features), handler)
}
