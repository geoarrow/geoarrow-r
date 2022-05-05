
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

test_that("as_arrow_array() can specify type", {
  skip_if_not(has_arrow_with_extension_type())

  array_pt <- arrow::as_arrow_array(
    wk::xy(1:5, 6:10, crs = "EPSG:1234"),
    type = GeoArrowType$create(geoarrow_schema_wkb())
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
