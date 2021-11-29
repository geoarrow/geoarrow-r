
test_that("extension schemas can be created", {
  expect_s3_class(geo_arrow_schema_wkb(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_wkt(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_geojson(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_point(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_point_float32(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_point_s2(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_point_h3(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_linestring(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_polygon(), "carrow_schema")

  expect_s3_class(geo_arrow_schema_multi(geo_arrow_schema_point()), "carrow_schema")
  expect_s3_class(geo_arrow_schema_multi(geo_arrow_schema_linestring()), "carrow_schema")
  expect_s3_class(geo_arrow_schema_multi(geo_arrow_schema_polygon()), "carrow_schema")

  expect_s3_class(geo_arrow_schema_dense_geometrycollection(list(geo_arrow_schema_point())), "carrow_schema")
  expect_s3_class(geo_arrow_schema_sparse_geometrycollection(list(geo_arrow_schema_point())), "carrow_schema")

  expect_s3_class(geo_arrow_schema_flat_linestring(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_flat_polygon(), "carrow_schema")
  expect_s3_class(geo_arrow_schema_flat_geometrycollection(geo_arrow_schema_point()), "carrow_schema")
})
