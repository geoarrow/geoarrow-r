
test_that("geoarrow_cast() does nothing when schemas are identical", {
  array <- geoarrow_create_narrow(
    wk::wkt("POINT (0 1)", "POINT (0 2)"),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(geoarrow_cast(array, array$schema), array)
})

test_that("geoarrow_cast() can cast all the examples to WKT", {
  # nc has coordinates that take up all 16 precision slots,
  # which have some minor differences between the ryu and sprintf translations
  for (name in setdiff(names(geoarrow_example_wkt), character())) {
    src_narrow <- geoarrow_create_narrow(geoarrow_example_wkt[[name]])
    dst_narrow <- geoarrow_cast(src_narrow, geoarrow_schema_wkt())

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
