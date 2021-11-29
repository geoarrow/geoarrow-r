
test_that("extension metadata serialization can roundtrip values", {
  serialized <- geoarrow_metadata_serialize(crs = "one", ellipsoidal = TRUE, dim = "xyz")
  expect_identical(
    geoarrow_metadata_deserialize(serialized),
    list(crs = "one", ellipsoidal = "true", dim = "xyz")
  )
})
