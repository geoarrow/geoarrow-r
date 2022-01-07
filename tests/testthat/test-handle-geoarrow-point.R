
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

})
