
test_that("geoarrow_compute_handler() creates a new wk_handler", {
  expect_s3_class(geoarrow_compute_handler("void"), "geoarrow_compute_handler")

  expect_error(
    geoarrow_compute_handler("void", NULL),
    "must be an object created with"
  )

  expect_error(
    geoarrow_compute_handler("cast", narrow::narrow_schema("i")),
    "Unsupported extension type"
  )

  expect_error(
    geoarrow_compute_handler("not an op!", narrow::narrow_array()),
    "Unsupported operation: 'not an op!'"
  )

  expect_error(
    .Call(geoarrow_c_compute_handler_new, 100L, narrow::narrow_schema("n"), narrow::narrow_array()),
    "Unsupported operation: 100"
  )
})

test_that("geoarrow_compute_handler() can cast to geoarrow.wkt", {
  # zero-length
  result_narrow <- expect_s3_class(
    wk::wk_handle(
      wk::wkt(character(), crs = NULL),
      geoarrow_compute_handler("cast", geoarrow_schema_wkt())
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
      geoarrow_compute_handler("cast", geoarrow_schema_wkt())
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
      geoarrow_compute_handler("cast", geoarrow_schema_wkt())
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
      geoarrow_compute_handler("cast", geoarrow_schema_wkt())
    ),
    "narrow_array"
  )

  expect_identical(
    narrow::from_narrow_array(result_narrow),
    unclass(points)
  )
})

test_that("geoarrow_compute_handler() can roundtrip all example WKT", {
  # nc has coordinates that take up all 16 precision slots,
  # which have some minor differences between the ryu and sprintf translations
  for (name in setdiff(names(geoarrow_example_wkt), "nc")) {
    result_narrow <- wk::wk_handle(
      geoarrow_example_wkt[[name]],
      geoarrow_compute_handler("cast", geoarrow_schema_wkt())
    )

    expect_identical(
      narrow::from_narrow_array(result_narrow),
      as.character(geoarrow_example_wkt[[!! name]])
    )
  }

  # check that nc coord values are equal at 16 digits
  result_narrow <- wk::wk_handle(
    geoarrow_example_wkt[["nc"]],
    geoarrow_compute_handler("cast", geoarrow_schema_wkt())
  )

  expect_identical(
    wk::wk_format(
      wk::wkt(narrow::from_narrow_array(result_narrow)),
      precision = 16, max_coords = .Machine$integer.max
    ),
    wk::wk_format(
      geoarrow_example_wkt[["nc"]],
      precision = 16, max_coords = .Machine$integer.max
    ),
  )
})

test_that("geoarrow_compute_handler() can read all examples using the null builder", {
  for (name in names(geoarrow_example_wkt)) {
    result_narrow <- wk::wk_handle(
      geoarrow_example_wkt[[name]],
      geoarrow_compute_handler("void")
    )

    expect_identical(result_narrow$schema$format, "n")
    expect_identical(result_narrow$array_data$length, 0L)
  }
})
