
handle_geoarrow_point <- function(array, handler) {
  handle_geoarrow_point_stream(
    carrow::as_carrow_array_stream(array),
    handler,
    array$schema,
    n_features = array$array_data$length
  )
}

handle_geoarrow_point_stream <- function(array_stream, handler,
                                                schema = carrow::carrow_array_stream_get_schema(array_stream),
                                                n_features = NA_integer_) {
  .Call(geoarrow_c_handle_point, list(array_stream, schema, n_features), handler)
}
