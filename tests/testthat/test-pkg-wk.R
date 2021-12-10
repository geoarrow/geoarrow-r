
test_that("wk_crs() works", {
  arr <- geoarrow_create(wk::wkt("POINT (1 2)"), schema = geoarrow_schema_wkt(crs = "1234"))
  expect_identical(wk_crs(arr), "1234")

  arr <- geoarrow_create(wk::wkt("POINT (1 2)"), schema = geoarrow_schema_wkt())
  expect_null(wk_crs(arr))
})
