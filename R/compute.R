
geoarrow_compute <- function(x, op = "void", schema = narrow::narrow_schema("n")) {
  x <- narrow::as_narrow_array(x)
  op <- geoarrow_compute_op(op)
  schema <- narrow::as_narrow_schema(schema)

  if (op == 1L) {
    schemas_identical <- identical(
      narrow::narrow_schema_info(x$schema, recursive = TRUE),
      narrow::narrow_schema_info(schema)
    )

    if (schemas_identical) {
      return(x)
    }
  }

  array_out <- narrow::narrow_array(
    schema,
    narrow::narrow_allocate_array_data(),
    validate = FALSE
  )

  .Call(geoarrow_c_compute, op, x, array_out)
}

geoarrow_compute_op <- function(op) {
  stopifnot(is.character(op), length(op) == 1)
  switch(
    tolower(op),
    "void" = 0L,
    "cast" = 1L,
    "global_bounds" = 2L,
    stop(sprintf("Unsupported operation: '%s'", op))
  )
}
