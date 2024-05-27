
test_that("handler works on geoarrow.point", {
  expect_identical(
    geoarrow_handle(wk::xy(0:1999, 1:2000), wk::xy_writer()),
    wk::xy(0:1999, 1:2000)
  )

  expect_identical(
    geoarrow_handle(wk::xyz(0, 1, 2), wk::xy_writer()),
    wk::xyz(0, 1, 2)
  )

  expect_identical(
    geoarrow_handle(wk::xym(0, 1, 3), wk::xy_writer()),
    wk::xym(0, 1, 3)
  )

  expect_identical(
    geoarrow_handle(wk::xyzm(0, 1, 2, 3), wk::xy_writer()),
    wk::xyzm(0, 1, 2, 3)
  )
})

test_that("geoarrow_writer() works for XY", {
  array <- wk::wk_handle(wk::xy(0:2, 1:3), geoarrow_writer(na_extension_wkt()))
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  expect_identical(
    schema$metadata[["ARROW:extension:name"]],
    "geoarrow.wkt"
  )

  nanoarrow::nanoarrow_array_set_schema(array, nanoarrow::na_string())
  expect_identical(
    nanoarrow::convert_array(array),
    c("POINT (0 1)", "POINT (1 2)", "POINT (2 3)")
  )
})

test_that("geoarrow_writer() works for XYZ", {
  array <- wk::wk_handle(
    wk::xyz(0:2, 1:3, 2:4),
    geoarrow_writer(na_extension_wkt())
  )
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  expect_identical(
    schema$metadata[["ARROW:extension:name"]],
    "geoarrow.wkt"
  )

  nanoarrow::nanoarrow_array_set_schema(array, nanoarrow::na_string())
  expect_identical(
    nanoarrow::convert_array(array),
    c("POINT Z (0 1 2)", "POINT Z (1 2 3)", "POINT Z (2 3 4)")
  )
})

test_that("geoarrow_writer() works for XYM", {
  array <- wk::wk_handle(
    wk::xym(0:2, 1:3, 2:4),
    geoarrow_writer(na_extension_wkt())
  )
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  expect_identical(
    schema$metadata[["ARROW:extension:name"]],
    "geoarrow.wkt"
  )

  nanoarrow::nanoarrow_array_set_schema(array, nanoarrow::na_string())
  expect_identical(
    nanoarrow::convert_array(array),
    c("POINT M (0 1 2)", "POINT M (1 2 3)", "POINT M (2 3 4)")
  )
})

test_that("geoarrow_writer() works for XYZM", {
  array <- wk::wk_handle(
    wk::xyzm(0:2, 1:3, 2:4, 3:5),
    geoarrow_writer(na_extension_wkt())
  )
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  expect_identical(
    schema$metadata[["ARROW:extension:name"]],
    "geoarrow.wkt"
  )

  nanoarrow::nanoarrow_array_set_schema(array, nanoarrow::na_string())
  expect_identical(
    nanoarrow::convert_array(array),
    c("POINT ZM (0 1 2 3)", "POINT ZM (1 2 3 4)", "POINT ZM (2 3 4 5)")
  )
})

test_that("handle_geoarrow() can roundtrip wk examples as WKT", {
  for (ex_name in setdiff(names(wk::wk_example_wkt), "nc")) {
    example <- wk::wk_example_wkt[[ex_name]]
    chars <- nchar(as.character(example))
    chars[is.na(example)] <- 0L
    array <- geoarrow_array_from_buffers(
      na_extension_wkt(),
      list(
        !is.na(example),
        c(0L, cumsum(chars)),
        as.character(example)
      )
    )

    # Check the array was constructed properly
    expect_identical(
      nanoarrow::convert_array(force_array_storage(array)),
      unclass(example)
    )

    # Check that the handler can recreate it with the wkt writer
    expect_identical(
      geoarrow_handle(array, wk::wkt_writer()),
      example
    )
  }
})

test_that("geoarrow_writer() can roundtrip wk examples as WKT", {
  for (ex_name in setdiff(names(wk::wk_example_wkt), "nc")) {
    example <- wk::wk_example_wkt[[ex_name]]

    # GeoArrow uses flat multipoint
    if (grepl("multipoint", ex_name)) {
      example <- wk::wkt(gsub("\\(([0-9 ]+)\\)", "\\1", as.character(example)))
    }

    array <- wk::wk_handle(example, geoarrow_writer(na_extension_wkt()))
    storage_convert <- nanoarrow::convert_array(force_array_storage(array))
    expect_identical(wk::wkt(storage_convert), example)
  }
})
