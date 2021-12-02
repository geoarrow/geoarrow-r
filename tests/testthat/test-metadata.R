
test_that("extension metadata serialization can roundtrip values", {
  serialized <- geoarrow_metadata_serialize(crs = "one", geodesic = TRUE, dim = "xyz")
  expect_identical(
    geoarrow_metadata_deserialize(serialized),
    list(crs = "one", geodesic = "true", dim = "xyz")
  )
})
