
test_that("wk_crs() works", {
  arr <- geoarrow_create(wk::wkt("POINT (1 2)", crs = "OGC:CRS84"))
  expect_identical(wk_crs(arr), "OGC:CRS84")
  expect_false(wk_is_geodesic(arr))

  arr <- geoarrow_create(wk::wkt("LINESTRING (1 2, 3 4)", geodesic = TRUE))
  expect_true(wk::wk_is_geodesic(arr))

  arr <- geoarrow_create(wk::wkt("POINT (1 2)"), schema = geoarrow_schema_wkt())
  expect_null(wk_crs(arr))

  arr <- geoarrow_create(
    wk::wkt("LINESTRING (1 2, 3 4)"),
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point(crs = "1234")
    )
  )
  expect_identical(wk_crs(arr), "1234")

  arr <- geoarrow_create(wk::wkt("LINESTRING (1 2, 3 4)"))
  expect_null(wk_crs(arr))
})
