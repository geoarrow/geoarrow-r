
test_that("geoarrow point reader works for linestring<xy>", {
  features <- wk::wk_linestring(
    wk::xy(1:10, 11:20),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(features, schema = geoarrow_schema_linestring())

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = FALSE, has_m = FALSE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})

test_that("geoarrow point reader works for linestring<xyz>", {
  features <- wk::wk_linestring(
    wk::xyz(1:10, 11:20, 21:30),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(
    features,
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point(dim = "xyz", nullable = FALSE)
    )
  )

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = TRUE, has_m = FALSE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})

test_that("geoarrow point reader works for linestring<xym>", {
  features <- wk::wk_linestring(
    wk::xym(1:10, 11:20, 31:40),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(
    features,
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point(dim = "xym", nullable = FALSE)
    )
  )

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = FALSE, has_m = TRUE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})

test_that("geoarrow point reader works for linestring<xyzm>", {
  features <- wk::wk_linestring(
    wk::xyzm(1:10, 11:20, 21:30, 31:40),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(
    features,
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point(dim = "xyzm", nullable = FALSE)
    )
  )

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = TRUE, has_m = TRUE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})

test_that("geoarrow point reader works for linestring<struct xy>", {
  features <- wk::wk_linestring(
    wk::xy(1:10, 11:20),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(
    features,
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point_struct(dim = "xy", nullable = FALSE)
    )
  )

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = FALSE, has_m = FALSE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})

test_that("geoarrow point reader works for linestring<struct xyz>", {
  features <- wk::wk_linestring(
    wk::xyz(1:10, 11:20, 21:30),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(
    features,
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point_struct(dim = "xyz", nullable = FALSE)
    )
  )

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = TRUE, has_m = FALSE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})

test_that("geoarrow point reader works for linestring<struct xym>", {
  features <- wk::wk_linestring(
    wk::xym(1:10, 11:20, 31:40),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(
    features,
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point_struct(dim = "xym", nullable = FALSE)
    )
  )

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = FALSE, has_m = TRUE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})

test_that("geoarrow point reader works for linestring<struct xyzm>", {
  features <- wk::wk_linestring(
    wk::xyzm(1:10, 11:20, 21:30, 31:40),
    feature_id = c(rep(1, 3), rep(2, 7))
  )
  features_array <- geoarrow_create(
    features,
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point_struct(dim = "xyzm", nullable = FALSE)
    )
  )

  expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
  expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
  expect_identical(
    wk::wk_vector_meta(features_array),
    data.frame(geometry_type = 2L, size = 2, has_z = TRUE, has_m = TRUE)
  )
  expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
})
