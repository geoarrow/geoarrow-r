
test_that("geoarrow_compute() errors for invalid operation", {
  expect_error(
    geoarrow_compute(geoarrow_example_narrow("point"), "not an op!"),
    "Unknown operation: 'not an op!'"
  )
})

test_that("geoarrow_compute() can cast all the examples to WKT", {
  # nc has coordinates that take up all 16 precision slots,
  # which have some minor differences between the ryu and sprintf translations
  for (name in setdiff(names(geoarrow_example_wkt), character())) {
    src_narrow <- geoarrow_create_narrow_from_buffers(geoarrow_example_wkt[[name]])
    dst_narrow <- geoarrow_compute(
      src_narrow,
      "cast",
      list(schema = geoarrow_schema_wkt())
    )

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

test_that("geoarrow_compute() can cast all the examples to WKB", {
  skip_if_not_installed("geos")

  for (name in names(geoarrow_example_wkt)) {
    src_narrow <- geoarrow_create_narrow_from_buffers(geoarrow_example_wkt[[name]])
    dst_narrow <- geoarrow_compute(
      src_narrow,
      "cast",
      list(schema = geoarrow_schema_wkb())
    )

    dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.wkb")

    expect_identical(
      wk::wk_count(dst_narrow),
      wk::wk_count(wk::as_wkb(geoarrow_example_wkt[[name]]))
    )

    expect_identical(
      wk::wk_coords(dst_narrow),
      wk::wk_coords(wk::as_wkb(geoarrow_example_wkt[[name]]))
    )
  }
})

test_that("geoarrow_compute(op = 'global_bounds') works for all examples", {
  for (name in names(geoarrow_example_wkt)) {
    src_wkt <- geoarrow_example_wkt[[name]]

    # with null_is_empty = FALSE
    result_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "global_bounds",
      list(null_is_empty = FALSE)
    )

    result_r <- narrow::from_narrow_array(result_narrow)
    expect_named(
      result_r,
      c("xmin", "xmax", "ymin", "ymax", "zmin", "zmax", "mmin", "mmax")
    )

    if (any(is.na(src_wkt))) {
      expect_identical(
        unlist(result_r, use.names = FALSE),
        rep(NaN, 8)
      )
    } else {
      bbox <- wk::wk_bbox(src_wkt)
      result_r <- result_r[names(unclass(bbox))]
      attributes(bbox) <- NULL
      attributes(result_r) <- NULL

      expect_identical(result_r, bbox)
    }

    # with null_is_empty = TRUE
    result_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "global_bounds",
      list(null_is_empty = TRUE)
    )

    result_r <- narrow::from_narrow_array(result_narrow)
    bbox <- wk::wk_bbox(src_wkt[!is.na(src_wkt)])
    result_r <- result_r[names(unclass(bbox))]
    attributes(bbox) <- NULL
    attributes(result_r) <- NULL

    expect_identical(result_r, bbox)
  }
})

test_that("geoarrow_compute(op = 'void') can handle all examples", {
  for (name in names(geoarrow_example_wkt)) {
    result_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(geoarrow_example_wkt[[name]]),
      "void"
    )

    expect_identical(result_narrow$schema$format, "n")
    expect_identical(result_narrow$array_data$length, 0L)
  }
})
