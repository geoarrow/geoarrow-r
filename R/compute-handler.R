
geoarrow_compute_handler <- function(op = "void", options = list()) {
  op <- geoarrow_compute_op(op)

  array_out <- narrow::narrow_array(
    narrow::narrow_allocate_schema(),
    narrow::narrow_allocate_array_data(),
    validate = FALSE
  )

  wk::new_wk_handler(
    .Call(geoarrow_c_compute_handler_new, op, array_out, options),
    "geoarrow_compute_handler"
  )
}
