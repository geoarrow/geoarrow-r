
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
