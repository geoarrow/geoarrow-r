
test_that("geoarrow_schema_default() works with and without wk::wk_vector_meta()", {
  schema_point <- geoarrow_schema_default(wk::xy(1:2, 1:2))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow::point")

  schema_point_crs <- geoarrow_schema_default(wk::xy(1:2, 1:2, crs = "EPSG:3857"))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow::point")
  geoarrow_meta <- geoarrow_metadata(schema_point_crs)
  expect_identical(geoarrow_meta$crs, "EPSG:3857")

  schema_wkt <- geoarrow_schema_default(wk::as_wkt(wk::xy(1:2, 1:2)))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow::point")
})

test_that("geoarrow_schema_default() detects dimensions from vector_meta", {
  schema_point <- geoarrow_schema_default(wk::xy(1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point)
  expect_identical(geoarrow_meta$dim, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::xyz(1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyz)
  expect_identical(geoarrow_meta$dim, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::xym(1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xym)
  expect_identical(geoarrow_meta$dim, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::xyzm(1:2, 1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyzm)
  expect_identical(geoarrow_meta$dim, "xyzm")
})

test_that("geoarrow_schema_default() detects dimensions from meta", {
  schema_point <- geoarrow_schema_default(wk::as_wkt(wk::xy(1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point)
  expect_identical(geoarrow_meta$dim, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::as_wkt(wk::xyz(1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyz)
  expect_identical(geoarrow_meta$dim, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::as_wkt(wk::xym(1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xym)
  expect_identical(geoarrow_meta$dim, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::as_wkt(wk::xyzm(1:2, 1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyzm)
  expect_identical(geoarrow_meta$dim, "xyzm")
})
