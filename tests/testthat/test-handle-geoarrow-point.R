
test_that("geoarrow point reader works", {
  points <- wk::xy(1:10, 11:20)
  points_array <- geoarrow_create(points, schema = geoarrow_schema_point())
  wk_handle(points_array, wk::wk_debug_filter())
})
