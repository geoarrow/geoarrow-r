
test_that("extension schemas can be created", {
  expect_s3_class(geoarrow_schema_wkb(), "narrow_schema")
  expect_s3_class(geoarrow_schema_wkt(), "narrow_schema")

  expect_s3_class(geoarrow_schema_point(), "narrow_schema")
  expect_s3_class(geoarrow_schema_linestring(), "narrow_schema")
  expect_s3_class(geoarrow_schema_polygon(), "narrow_schema")

  expect_s3_class(geoarrow_schema_multipoint(), "narrow_schema")
  expect_s3_class(geoarrow_schema_multilinestring(), "narrow_schema")
  expect_s3_class(geoarrow_schema_multipolygon(), "narrow_schema")
})
