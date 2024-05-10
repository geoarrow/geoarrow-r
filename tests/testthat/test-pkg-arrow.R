
test_that("arrow extension type methods return correct values", {
  skip_if_not_installed("arrow")

  type <- arrow::as_data_type(
    na_extension_geoarrow("POINT", crs = "EPSG:1234", edges = "SPHERICAL")
  )
  expect_s3_class(type, "GeometryExtensionType")
  expect_identical(type$geoarrow_id, 1L)
  expect_identical(type$geometry_type, 1L)
  expect_identical(type$extension_name(), "geoarrow.point")
  expect_identical(type$crs, "EPSG:1234")
  expect_identical(type$dimensions, "XY")
  expect_identical(type$coord_type, "SEPARATE")
  expect_identical(type$edge_type, "SPHERICAL")
})

test_that("arrow extension type method has a reasonable ToString() method", {
  skip_if_not_installed("arrow")

  type <- arrow::as_data_type(
    na_extension_geoarrow("POINT", crs = "EPSG:1234", edges = "SPHERICAL")
  )

  expect_identical(
    type$ToString(),
    "spherical geoarrow.point <CRS: EPSG:1234>"
  )

  type_long_crs <- arrow::as_data_type(
    na_extension_geoarrow(
      "POINT",
      crs = strrep("a", 100),
      edges = "SPHERICAL"
    )
  )

  expect_identical(
    type_long_crs$ToString(),
    "spherical geoarrow.point <CRS: aaaaaaaaaaaaaaaaaaaaaaaaaaa..."
  )
})

test_that("as_chunked_array() works for geoarrow_vctr", {
  skip_if_not_installed("arrow")

  vctr <- as_geoarrow_vctr("POINT (0 1)")
  chunked <- arrow::as_chunked_array(vctr)
  expect_s3_class(chunked, "ChunkedArray")
  expect_equal(chunked$length(), 1)
  expect_s3_class(chunked$type, "GeometryExtensionType")
  expect_identical(chunked$type$extension_name(), "geoarrow.wkt")
  expect_identical(chunked$chunk(0)$storage()$as_vector(), "POINT (0 1)")

  # Check conversion back to R
  vctr_roundtrip <- as.vector(chunked)
  expect_s3_class(vctr_roundtrip, "geoarrow_vctr")
  expect_identical(format(vctr_roundtrip), "<POINT (0 1)>")

  # Check with a requested type
  chunked <- arrow::as_chunked_array(vctr, type = na_extension_wkb())
  expect_equal(chunked$length(), 1)
  expect_s3_class(chunked$type, "GeometryExtensionType")
  expect_identical(chunked$type$extension_name(), "geoarrow.wkb")
})

test_that("as_chunked_array() works for a sliced geoarrow_vctr", {
  skip_if_not_installed("arrow")

  vctr <- as_geoarrow_vctr(wk::xy(1:10, 11:20))[8:10]
  chunked <- arrow::as_chunked_array(vctr)
  expect_identical(chunked$length(), 3L)
})

test_that("as_arrow_array() works for geoarrow_vctr", {
  skip_if_not_installed("arrow")

  vctr <- as_geoarrow_vctr("POINT (0 1)")
  array <- arrow::as_arrow_array(vctr)
  expect_s3_class(array, "Array")
  expect_equal(array$length(), 1)

  vctr2 <- as_geoarrow_vctr(
    nanoarrow::basic_array_stream(
      list(
        as_geoarrow_array("POINT (0 1)"),
        as_geoarrow_array("POINT (1 2)")
      ),
      schema = na_extension_wkt()
    )
  )

  array <- arrow::as_arrow_array(vctr2)
  expect_s3_class(array, "Array")
  expect_equal(array$length(), 2)

  # Check with a requested type
  array <- arrow::as_arrow_array(vctr2, type = na_extension_wkb())
})

test_that("infer_type() works for geoarrow_vctr", {
  skip_if_not_installed("arrow")

  vctr <- as_geoarrow_vctr("POINT (0 1)")
  type <- arrow::infer_type(vctr)
  expect_identical(type$extension_name(), "geoarrow.wkt")
})
