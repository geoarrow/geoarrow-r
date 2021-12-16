
handle_geoarrow_wkt <- function(array, handler) {
  chr <- carrow::from_carrow_array(array, character())
  wk::wk_handle(wk::new_wk_wkt(chr), handler)
}
