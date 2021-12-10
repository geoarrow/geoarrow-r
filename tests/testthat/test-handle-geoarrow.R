
test_that("wk_handle() works for geoarrow.wkt", {
  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  arr <- geoarrow_create(src, schema = geoarrow_schema_wkt())
  expect_identical(wk::wk_handle(arr, wk::wkt_writer()), src)
})

test_that("wk_handle() works for geoarrow.geojson", {
  skip_if_not_installed("geos")

  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  src_geojson <- geos::geos_write_geojson(src)
  arr <- geoarrow_create(src, schema = geoarrow_schema_geojson())
  expect_identical(wk::wk_handle(arr, wk::wkt_writer()), src)
})

test_that("wk_handle() works for geoarrow.wkb", {
  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  src_wkb <- wk::as_wkb(src)
  arr <- geoarrow_create(src, schema = geoarrow_schema_wkb())
  expect_identical(wk::wk_handle(arr, wk::wkt_writer()), src)
})
