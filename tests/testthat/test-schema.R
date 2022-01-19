
test_that("extension schemas can be created", {
  expect_s3_class(geoarrow_schema_wkb(), "sparrow_schema")
  expect_s3_class(geoarrow_schema_wkt(), "sparrow_schema")
  expect_s3_class(geoarrow_schema_geojson(), "sparrow_schema")
  expect_s3_class(geoarrow_schema_point_struct(), "sparrow_schema")
  expect_s3_class(geoarrow_schema_point(), "sparrow_schema")
  expect_s3_class(geoarrow_schema_linestring(), "sparrow_schema")
  expect_s3_class(geoarrow_schema_polygon(), "sparrow_schema")

  expect_s3_class(geoarrow_schema_multi(geoarrow_schema_point()), "sparrow_schema")
  expect_s3_class(geoarrow_schema_multi(geoarrow_schema_linestring()), "sparrow_schema")
  expect_s3_class(geoarrow_schema_multi(geoarrow_schema_polygon()), "sparrow_schema")

  expect_s3_class(geoarrow_schema_dense_geometrycollection(list(geoarrow_schema_point())), "sparrow_schema")
  expect_s3_class(geoarrow_schema_sparse_geometrycollection(list(geoarrow_schema_point())), "sparrow_schema")
})
