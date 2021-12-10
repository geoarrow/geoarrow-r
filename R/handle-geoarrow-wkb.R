
handle_geoarrow_wkb <- function(array, handler) {
  .Call(geoarrow_c_handle_wkb, array, handler)
}
