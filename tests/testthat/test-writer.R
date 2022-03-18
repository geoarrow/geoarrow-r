
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
  expect_null(
    wk::wk_handle(
      wk::wkt("POINT (0 1)"),
      geoarrow_writer(geoarrow_schema_wkt())
    )
  )
})
