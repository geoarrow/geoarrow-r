
test_that("extension metadata serialization can roundtrip values", {
  serialized <- geo_arrow_metadata_serialize(crs = "one", ellipsoidal = TRUE, dim = "xyz")
  expect_identical(
    geo_arrow_metadata_deserialize(serialized),
    list(crs = "one", ellipsoidal = "true", dim = "xyz")
  )
})
