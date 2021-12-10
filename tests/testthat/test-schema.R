
test_that("extension schemas can be created", {
  expect_s3_class(geoarrow_schema_wkb(), "carrow_schema")
  expect_s3_class(geoarrow_schema_wkt(), "carrow_schema")
  expect_s3_class(geoarrow_schema_geojson(), "carrow_schema")
  expect_s3_class(geoarrow_schema_point_struct(), "carrow_schema")
  expect_s3_class(geoarrow_schema_point(), "carrow_schema")
  expect_s3_class(geoarrow_schema_linestring(), "carrow_schema")
  expect_s3_class(geoarrow_schema_polygon(), "carrow_schema")

  expect_s3_class(geoarrow_schema_multi(geoarrow_schema_point()), "carrow_schema")
  expect_s3_class(geoarrow_schema_multi(geoarrow_schema_linestring()), "carrow_schema")
  expect_s3_class(geoarrow_schema_multi(geoarrow_schema_polygon()), "carrow_schema")

  expect_s3_class(geoarrow_schema_dense_geometrycollection(list(geoarrow_schema_point())), "carrow_schema")
  expect_s3_class(geoarrow_schema_sparse_geometrycollection(list(geoarrow_schema_point())), "carrow_schema")
})
