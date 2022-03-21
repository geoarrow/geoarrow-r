
test_that("geoarrow_writer() creates a new wk_handler", {
  expect_s3_class(geoarrow_writer(geoarrow_schema_wkt()), "geoarrow_writer")

  expect_error(
    geoarrow_writer(NULL),
    "must be an object created with"
  )

  expect_error(
    geoarrow_writer(narrow::narrow_schema("i")),
    "Unsupported extension type"
  )
})

test_that("geoarrow_writer() can write geoarrow.wkt", {
  for (name in names(geoarrow_example_wkt)) {}



  result <- expect_s3_class(
    wk::wk_handle(
      wk::wkt("POINT (0 1)"),
      geoarrow_writer(geoarrow_schema_wkt())
    ),
    "narrow_array"
  )

  expect_identical(
    narrow::from_narrow_array(result),
    "POINT (0 1)"
  )

  result <- expect_s3_class(
    wk::wk_handle(
      wk::wkt(c("POINT (0 1)", "POINT (1.1 2.2)")),
      geoarrow_writer(geoarrow_schema_wkt())
    ),
    "narrow_array"
  )

  expect_identical(
    narrow::from_narrow_array(result),
    c("POINT (0 1)", "POINT (1.1 2.2)")
  )

  result <- wk::wk_handle(
    geoarrow_example_wkt$linestring,
    geoarrow_writer(geoarrow_schema_wkt())
  )

  narrow::from_narrow_array(result)

  nc_narrow <- wk::wk_handle(
    geoarrow_example_wkt$nc,
    geoarrow_writer(geoarrow_schema_wkt())
  )

  expect_identical(
    narrow::from_narrow_array(nc_narrow),
    as.character(geoarrow_example_wkt$nc)
  )
})
