
test_that("extension metadata serialization can roundtrip values", {
  serialized <- geoarrow_metadata_serialize(crs = "one", geodesic = TRUE, dim = "xyz")
  expect_identical(
    geoarrow_metadata_deserialize(serialized),
    list(crs = "one", geodesic = "true", dim = "xyz")
  )
})

test_that("metadata can be updated for a schema", {
  schema <- narrow::narrow_schema("i")

  schema2 <- geoarrow_set_metadata(schema, dim = "xy")
  expect_identical(geoarrow_metadata(schema2)$dim, "xy")

  schema2 <- geoarrow_set_metadata(schema, crs = "OGC:CRS84")
  expect_identical(geoarrow_metadata(schema2)$crs, "OGC:CRS84")

  schema2 <- geoarrow_set_metadata(schema, geodesic = TRUE)
  expect_identical(geoarrow_metadata(schema2)$geodesic, "true")
})
