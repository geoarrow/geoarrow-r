
geoarrow_compute <- function(x, op = "void", options = list()) {
  x <- narrow::as_narrow_array(x)
  op <- geoarrow_compute_op(op)

  array_out <- narrow::narrow_array(
    narrow::narrow_allocate_schema(),
    narrow::narrow_allocate_array_data(),
    validate = FALSE
  )

  .Call(geoarrow_c_compute, op, x, array_out, options)
}

geoarrow_compute_op <- function(op) {
  stopifnot(is.character(op), length(op) == 1)
  op
}
