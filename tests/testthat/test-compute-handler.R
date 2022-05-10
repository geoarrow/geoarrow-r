
test_that("geoarrow_compute_handler() creates a new wk_handler", {
  expect_s3_class(geoarrow_compute_handler("void"), "geoarrow_compute_handler")

  expect_error(
    geoarrow_compute_handler("void", NULL),
    "must be a list"
  )

  expect_error(
    geoarrow_compute_handler("cast", list(schema = narrow::narrow_schema("i"))),
    "Unsupported extension type"
  )

  expect_error(
    geoarrow_compute_handler("not an op!"),
    "Unknown operation: 'not an op!'"
  )
})

test_that("geoarrow_compute_handler() can cast to geoarrow.wkt", {
  # zero-length
  result_narrow <- expect_s3_class(
    wk::wk_handle(
      wk::wkt(character(), crs = NULL),
      geoarrow_compute_handler("cast", list(schema = geoarrow_schema_wkt()))
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
      geoarrow_compute_handler("cast", list(schema = geoarrow_schema_wkt()))
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
      geoarrow_compute_handler("cast", list(schema = geoarrow_schema_wkt()))
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
      geoarrow_compute_handler("cast", list(schema = geoarrow_schema_wkt()))
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
      geoarrow_compute_handler("cast", list(schema = geoarrow_schema_wkt()))
    )

    expect_identical(
      narrow::from_narrow_array(result_narrow),
      as.character(geoarrow_example_wkt[[!! name]])
    )
  }

  # check that nc coord values are equal at 16 digits
  result_narrow <- wk::wk_handle(
    geoarrow_example_wkt[["nc"]],
    geoarrow_compute_handler("cast", list(schema = geoarrow_schema_wkt()))
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

test_that("geoarrow_compute_handler() can cast all the examples to WKB", {
  for (name in names(geoarrow_example_wkt)) {
    dst_narrow <- wk::wk_handle(
      geoarrow_example_wkt[[name]],
      geoarrow_compute_handler("cast", list(schema = geoarrow_schema_wkb()))
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

test_that("geoarrow_compute_handler(op = 'global_bounds') works for all examples", {
  for (name in names(geoarrow_example_wkt)) {
    src_wkt <- geoarrow_example_wkt[[name]]

    # with null_is_empty = FALSE
    result_narrow <- wk::wk_handle(
      src_wkt,
      geoarrow_compute_handler(
        "global_bounds",
        list(null_is_empty = FALSE)
      )
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
      geoarrow_create_narrow(src_wkt),
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
