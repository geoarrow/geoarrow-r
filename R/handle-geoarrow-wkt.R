
geoarrow_create_wkt <- function(x, ...) {
  geoarrow_create(wk::new_wk_wkt(x), schema = geoarrow_schema_wkt(...))
}

handle_geoarrow_wkt <- function(array, handler) {
  .Call(geoarrow_c_handle_wkt, array, handler)
}
