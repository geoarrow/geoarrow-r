
test_that("as_arrow_array() works for geoarrow_vctr", {
  skip_if_not(has_arrow_with_extension_type())

  # with default type
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  array <- arrow::as_arrow_array(vctr)
  expect_s3_class(array, "Array")
  expect_true(array$type$extension_name() == "geoarrow.point")
  expect_identical(wk::as_xy(as_geoarrow(array)), wk::xy(1:4, 5:8))

  # with identical type
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  array <- arrow::as_arrow_array(vctr, type = arrow::infer_type(vctr))
  expect_s3_class(array, "Array")
  expect_true(array$type$extension_name() == "geoarrow.point")
  expect_identical(wk::as_xy(as_geoarrow(array)), wk::xy(1:4, 5:8))

  # with custom type
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  array <- arrow::as_arrow_array(vctr, type = geoarrow_wkb())
  expect_s3_class(array, "Array")
  expect_true(array$type$extension_name() == "geoarrow.wkb")
  expect_identical(wk::as_xy(as_geoarrow(array)), wk::xy(1:4, 5:8))
})

test_that("as_chunked_array() works for geoarrow_vctr", {
  skip_if_not(has_arrow_with_extension_type())

  # with default type
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  array <- arrow::as_chunked_array(vctr)
  expect_s3_class(array, "ChunkedArray")
  expect_true(array$type$extension_name() == "geoarrow.point")
  expect_identical(wk::as_xy(as_geoarrow(array)), wk::xy(1:4, 5:8))

  # with identical type
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  array <- arrow::as_chunked_array(vctr, type = arrow::infer_type(vctr))
  expect_s3_class(array, "ChunkedArray")
  expect_true(array$type$extension_name() == "geoarrow.point")
  expect_identical(wk::as_xy(as_geoarrow(array)), wk::xy(1:4, 5:8))

  # with custom type
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  array <- arrow::as_chunked_array(vctr, type = geoarrow_wkb())
  expect_s3_class(array, "ChunkedArray")
  expect_true(array$type$extension_name() == "geoarrow.wkb")
  expect_identical(wk::as_xy(as_geoarrow(array)), wk::xy(1:4, 5:8))
})

test_that("as_arrow_array() works for handleables", {
  skip_if_not(has_arrow_with_extension_type())

  array_pt <- arrow::as_arrow_array(wk::xy(1:5, 6:10, crs = "EPSG:1234"))
  expect_s3_class(array_pt$type, "GeoArrowType")
  expect_identical(array_pt$type$crs, "EPSG:1234")

  # make sure we can also go through Array$create() / record_batch()
  expect_true(
    arrow::Array$create(wk::xy(1:5, 6:10, crs = "EPSG:1234"))$type ==
      array_pt$type
  )

  expect_true(
    arrow::ChunkedArray$create(wk::xy(1:5, 6:10, crs = "EPSG:1234"))$type ==
      array_pt$type
  )

  expect_true(
    arrow::record_batch(a = wk::xy(1:5, 6:10, crs = "EPSG:1234"))$a$type ==
      array_pt$type
  )
})

test_that("as_arrow_array() can specify type for handleables", {
  skip_if_not(has_arrow_with_extension_type())

  array_pt <- arrow::as_arrow_array(
    wk::xy(1:5, 6:10),
    type = GeoArrowType$create(geoarrow_schema_wkb(crs = "EPSG:1234"))
  )

  expect_s3_class(array_pt$type, "GeoArrowType")
  expect_identical(array_pt$type$crs, "EPSG:1234")

  # make sure we can also go through Array$create() / ChunkedArray$create()
  expect_true(
    arrow::Array$create(
      wk::xy(1:5, 6:10, crs = "EPSG:1234"),
      type = GeoArrowType$create(array_pt$type)
    )$type ==
      array_pt$type
  )

  expect_true(
    arrow::ChunkedArray$create(
      wk::xy(1:5, 6:10, crs = "EPSG:1234"),
      type = GeoArrowType$create(array_pt$type)
    )$type ==
      array_pt$type
  )
})
