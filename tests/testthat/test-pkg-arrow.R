
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

test_that("vctr columns in tables can roundtrip through arrow", {
  skip_if_not(has_arrow_with_extension_type())

  batch <- arrow::record_batch(geom = geoarrow(wk::wkt("POINT (0 1)")))
  expect_s3_class(batch$schema$geom$type, "GeoArrowType")
  expect_s3_class(batch$geom$as_vector(), "geoarrow_vctr")
  expect_s3_class(as.data.frame(batch)$geom, "geoarrow_vctr")
  expect_identical(wk::as_wkt(as_geoarrow(batch$geom)), wk::wkt("POINT (0 1)"))

  table <- arrow::arrow_table(geom = geoarrow(wk::wkt("POINT (0 1)")))
  expect_s3_class(table$schema$geom$type, "GeoArrowType")
  expect_s3_class(table$geom$as_vector(), "geoarrow_vctr")
  expect_s3_class(as.data.frame(table)$geom, "geoarrow_vctr")
  expect_identical(wk::as_wkt(as_geoarrow(table$geom)), wk::wkt("POINT (0 1)"))

  tf <- tempfile()
  on.exit(unlink(tf))
  arrow::write_parquet(table, tf)
  table2 <- arrow::read_parquet(tf, as_data_frame = FALSE)
  expect_s3_class(table2$schema$geom$type, "GeoArrowType")
  expect_s3_class(table2$geom$as_vector(), "geoarrow_vctr")
  expect_s3_class(as.data.frame(table2)$geom, "geoarrow_vctr")
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
