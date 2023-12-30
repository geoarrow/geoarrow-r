
test_that("extension arrays infer the correct ptype for arrays", {
  array <- as_geoarrow_array("POINT (0 1)")
  ptype <- nanoarrow::infer_nanoarrow_ptype(array)
  expect_s3_class(ptype, "geoarrow_vctr")
  parsed <- geoarrow_schema_parse(ptype)
  expect_identical(parsed$id, enum$Type$WKT)
})

test_that("extension arrays are converted to the correct vector type", {
  array <- as_geoarrow_array("POINT (0 1)")
  vctr <- nanoarrow::convert_array(array)
  expect_s3_class(vctr, "geoarrow_vctr")
  expect_identical(wk::as_wkt(vctr), wk::wkt("POINT (0 1)"))

  expect_error(
    nanoarrow::convert_array(array, integer()),
    "Can't convert geoarrow extension array to object of class 'integer'"
  )
})

test_that("R vectors can be converted to the geoarrow array type", {
  array <- nanoarrow::as_nanoarrow_array(
    "POINT (0 1)",
    schema = na_extension_geoarrow("POINT")
  )

  schema <- infer_nanoarrow_schema(array)
  expect_identical(schema$format, "+s")
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.point")
})
