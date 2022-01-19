
test_that("geoarrow point reader works for point", {
  coords_base <- wk::xy(1:10, 11:20)

  # helpful for interactive debugging
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
    for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
      dims_exploded <- strsplit(coord_dim, "")[[1]]
      features <- wk::as_wkb(wk::as_xy(coords_base, dims = dims_exploded))

      features_array <- geoarrow_create(
        features,
        schema = point_schema(dim = coord_dim, nullable = TRUE),
        strict = TRUE
      )

      expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
      expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
      expect_identical(
        wk::wk_vector_meta(features_array),
        data.frame(
          geometry_type = 1L,
          size = 10,
          has_z = "z" %in% dims_exploded,
          has_m = "m" %in% dims_exploded
        )
      )
      expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
    }
  }
})

test_that("geoarrow point struct reader errors for invalid schemas", {
  schema <- geoarrow_schema_point_struct()
  schema$children <- list()
  points_array <- sparrow::sparrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "dimensions 'xy' to have 2 or more children")

  schema <- geoarrow_schema_point_struct()
  schema$children[[1]] <- sparrow::sparrow_schema("n")
  points_array <- sparrow::sparrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "had an unsupported storage type 'n'")

  schema <- geoarrow_schema_point_struct()
  schema$children[[1]] <- sparrow::sparrow_schema("+l")
  points_array <- sparrow::sparrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "child 0 has an invalid schema")

  schema <- geoarrow_schema_point()
  points_array <- sparrow::sparrow_array(
    schema,
    sparrow::sparrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "array with 1 buffer")

  schema <- geoarrow_schema_point()
  points_array <- sparrow::sparrow_array(
    schema,
    sparrow::sparrow_array_data(
      buffers = list(NULL)
    ),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "one child but found 0")
})
