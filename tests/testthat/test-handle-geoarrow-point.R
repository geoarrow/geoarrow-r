
test_that("geoarrow point reader works for xy", {
  points <- wk::xy(1:10, 11:20)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point())

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point reader works for xyz", {
  points <- wk::xyz(1:10, 11:20, 21:30)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point(dim = "xyz"))

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point reader works for xym", {
  points <- wk::xym(1:10, 11:20, 31:40)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point(dim = "xym"))

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point reader works for xyzm", {
  points <- wk::xyzm(1:10, 11:20, 21:30, 31:40)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point(dim = "xyzm"))

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point reader errors for invalid schemas", {
  schema <- geoarrow_schema_point()
  schema$format <- "n"
  points_array <- carrow::carrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "struct or a fixed-width list")

  schema <- geoarrow_schema_point()
  schema$children <- list()
  points_array <- carrow::carrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "one child but found 0")

  schema <- geoarrow_schema_point()
  schema$children[[1]] <- carrow::carrow_schema("n")
  points_array <- carrow::carrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "type Float64")

  schema <- geoarrow_schema_point()
  schema$children[[1]] <- carrow::carrow_schema("+l")
  points_array <- carrow::carrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "invalid child schema")

  schema <- geoarrow_schema_point()
  points_array <- carrow::carrow_array(
    schema,
    carrow::carrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "array with 1 buffer")

  schema <- geoarrow_schema_point()
  points_array <- carrow::carrow_array(
    schema,
    carrow::carrow_array_data(
      buffers = list(NULL)
    ),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "one child but found 0")
})

test_that("geoarrow point struct reader works for xy", {
  points <- wk::xy(1:10, 11:20)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point_struct())

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point struct reader works for xyz", {
  points <- wk::xyz(1:10, 11:20, 21:30)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point_struct(dim = "xyz"))

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point struct reader works for xym", {
  points <- wk::xym(1:10, 11:20, 31:40)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point_struct(dim = "xym"))

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point struct reader works for xyzm", {
  points <- wk::xyzm(1:10, 11:20, 21:30, 31:40)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point_struct(dim = "xyzm"))

  expect_identical(wk_handle(points_array, wk::xy_writer()), points)
  expect_identical(wk::as_wkt(points_array), wk::as_wkt(points))
  expect_identical(wk::wk_vector_meta(points_array), wk::wk_vector_meta(points))
  expect_identical(wk::wk_meta(points_array), wk::wk_meta(points))
})

test_that("geoarrow point struct reader errors for invalid schemas", {
  schema <- geoarrow_schema_point_struct()
  schema$children <- list()
  points_array <- carrow::carrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "dimensions 'xy' to have 2 or more children")

  schema <- geoarrow_schema_point_struct()
  schema$children[[1]] <- carrow::carrow_schema("n")
  points_array <- carrow::carrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "had an unsupported storage type 'n'")

  schema <- geoarrow_schema_point_struct()
  schema$children[[1]] <- carrow::carrow_schema("+l")
  points_array <- carrow::carrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(points_array), "child 0 has an invalid schema")

  schema <- geoarrow_schema_point()
  points_array <- carrow::carrow_array(
    schema,
    carrow::carrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "array with 1 buffer")

  schema <- geoarrow_schema_point()
  points_array <- carrow::carrow_array(
    schema,
    carrow::carrow_array_data(
      buffers = list(NULL)
    ),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "one child but found 0")
})
