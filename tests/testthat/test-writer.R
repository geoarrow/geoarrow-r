
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
  # zero-length
  result_narrow <- expect_s3_class(
    wk::wk_handle(
      wk::wkt(character(), crs = NULL),
      geoarrow_writer(geoarrow_schema_wkt())
    ),
    "narrow_array"
  )

  expect_identical(
    narrow::from_narrow_array(result_narrow),
    character()
  )

  # single length
  result_narrow <- expect_s3_class(
    wk::wk_handle(
      wk::wkt("POINT (0 1)"),
      geoarrow_writer(geoarrow_schema_wkt())
    ),
    "narrow_array"
  )

  expect_identical(
    narrow::from_narrow_array(result_narrow),
    "POINT (0 1)"
  )

  # null
  result_narrow <- expect_s3_class(
    wk::wk_handle(
      wk::wkt(NA_character_),
      geoarrow_writer(geoarrow_schema_wkt())
    ),
    "narrow_array"
  )

  expect_identical(
    narrow::from_narrow_array(result_narrow),
    NA_character_
  )

  # random assortment of nulls
  is_null <- as.logical(round(runif(1000)))
  points <- wk::as_wkt(wk::xy(1:1000, 1001:2000))
  points[is_null] <- wk::wkt(NA_character_)
  result_narrow <- expect_s3_class(
    wk::wk_handle(
      points,
      geoarrow_writer(geoarrow_schema_wkt())
    ),
    "narrow_array"
  )

  expect_identical(
    narrow::from_narrow_array(result_narrow),
    unclass(points)
  )
})

test_that("geoarrow_writer() can roundtrip all example WKT", {
  for (name in names(geoarrow_example_wkt)) {
    result_narrow <- wk::wk_handle(
      geoarrow_example_wkt[[name]],
      geoarrow_writer(geoarrow_schema_wkt())
    )

    expect_identical(
      narrow::from_narrow_array(result_narrow),
      as.character(geoarrow_example_wkt[[!! name]])
    )
  }
})
