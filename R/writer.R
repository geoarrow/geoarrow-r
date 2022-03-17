
geoarrow_writer <- function(schema) {
  wk::new_wk_handler(
    .Call(geoarrow_c_builder_handler_new, schema),
    "geoarrow_writer"
  )
}
