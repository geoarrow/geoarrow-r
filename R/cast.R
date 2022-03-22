
geoarrow_cast <- function(x, schema) {
  x <- narrow::as_narrow_array(x)
  schema <- narrow::as_narrow_schema(schema)

  schemas_identical <- identical(
    narrow::narrow_schema_info(x$schema, recursive = TRUE),
    narrow::narrow_schema_info(schema)
  )

  if (schemas_identical) {
    return(x)
  }

  array_out <- narrow::narrow_array(
    schema,
    narrow::narrow_allocate_array_data(),
    validate = FALSE
  )

  .Call(geoarrow_c_cast, x, array_out)
}
