
handle_geoarrow_wkt <- function(array, handler) {
  .Call(geoarrow_c_handle_wkt, array, handler)
}
