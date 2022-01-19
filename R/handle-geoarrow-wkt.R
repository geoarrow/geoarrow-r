
# for testing
geoarrow_create_wkt <- function(x, ...) {
  geoarrow_create(wk::new_wk_wkt(x), schema = geoarrow_schema_wkt(...))
}

handle_geoarrow_wkt <- function(array, handler) {
  handle_geoarrow_wkt_stream(
    sparrow::as_sparrow_array_stream(array),
    handler,
    array$schema,
    n_features = array$array_data$length
  )
}

handle_geoarrow_wkt_stream <- function(array_stream, handler,
                                       schema = sparrow::sparrow_array_stream_get_schema(array_stream),
                                       n_features = NA_integer_) {
  .Call(geoarrow_c_handle_wkt, list(array_stream, schema, n_features), handler)
}
