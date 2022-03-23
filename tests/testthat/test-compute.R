
test_that("geoarrow_compute() does nothing when schemas are identical", {
  array <- geoarrow_create_narrow(
    wk::wkt("POINT (0 1)", "POINT (0 2)"),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(geoarrow_compute(array, "cast", array$schema), array)
})

test_that("geoarrow_compute() errors for invalid operation", {
  expect_error(
    geoarrow_compute(narrow::narrow_array(), "not an op!"),
    "Unsupported operation: 'not an op!'"
  )

  expect_error(
    .Call(geoarrow_c_compute, 100L, narrow::narrow_array(), narrow::narrow_array()),
    "Unsupported operation: 100"
  )
})

test_that("geoarrow_compute() can cast all the examples to WKT", {
  # nc has coordinates that take up all 16 precision slots,
  # which have some minor differences between the ryu and sprintf translations
  for (name in setdiff(names(geoarrow_example_wkt), character())) {
    src_narrow <- geoarrow_create_narrow(geoarrow_example_wkt[[name]])
    dst_narrow <- geoarrow_compute(src_narrow, "cast", geoarrow_schema_wkt())

    # There are some inconsistencies that happen during the roundtrip,
    # (e.g., POINT EMPTY vs POINT (nan nan) vs POINT (NaN NaN),
    # LINESTRING EMPTY vs LINESTRING Z EMPTY) so we refilter everything through
    # wk's wkb writer to check equality
    dst_wkt <- wk::new_wk_wkt(narrow::from_narrow_array(dst_narrow))

    # Works for points, but doesn't check structure
    expect_identical(
      wk::wk_coords(dst_wkt),
      wk::wk_coords(src_narrow)
    )

    # Checks structure, but doesn't work for points
    if (!grepl("^point", name)) {
      expect_identical(
        wk::as_wkb(dst_wkt),
        wk::wk_handle(src_narrow, wk::wkb_writer())
      )
    }
  }
})

test_that("geoarrow_compute(op = 'void') can handle all examples", {
  for (name in names(geoarrow_example_wkt)) {
    result_narrow <- geoarrow_compute(
      geoarrow_create_narrow(geoarrow_example_wkt[[name]]),
      "void"
    )

    expect_identical(result_narrow$schema$format, "n")
    expect_identical(result_narrow$array_data$length, 0L)
  }
})
