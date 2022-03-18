
geoarrow_writer <- function(schema) {
  array_out <- narrow::narrow_array(
    narrow::narrow_allocate_schema(),
    narrow::narrow_allocate_array_data(),
    validate = FALSE
  )

  wk::new_wk_handler(
    .Call(geoarrow_c_builder_handler_new, schema, array_out),
    "geoarrow_writer"
  )
}
