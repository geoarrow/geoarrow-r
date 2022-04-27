
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

test_that("geoarrow_compute() generates correct metadata for points", {
  skip("for now")
  narrow_template <- geoarrow_create_narrow_from_buffers(
    wk::wkt("POINT (0 1)")
  )

  narrow_compute <- geoarrow_compute(
    narrow_template,
    "cast",
    list(schema = geoarrow_schema_point())
  )

  expect_identical(
    narrow::narrow_schema_info(narrow_compute$schema, recursive = TRUE),
    narrow::narrow_schema_info(narrow_template$schema, recursive = TRUE)
  )
})

test_that("geoarrow_compute() can cast point examples to point", {
  for (name in c("point", "point_z", "point_m", "point_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_point())
    )
    dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.point")

    expect_identical(
      wk::as_wkb(dst_narrow),
      wk::as_wkb(geoarrow_create_narrow_from_buffers(src_wkt))
    )
  }
})

test_that("geoarrow_compute() errors when casting points with multiple dims", {
  all_points <- c(
    geoarrow_example_wkt[["point"]],
    geoarrow_example_wkt[["point_z"]],
    geoarrow_example_wkt[["point_m"]],
    geoarrow_example_wkt[["point_zm"]]
  )

  expect_error(
    geoarrow_compute(
      geoarrow_create_narrow_from_buffers(
        all_points,
        schema = geoarrow_schema_wkb()
      ),
      "cast",
      list(schema = geoarrow_schema_point())
    ),
    "Point builder with multiple dimensions not implemented"
  )
})

test_that("geoarrow_compute() can cast (some) multipoint to point", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::wkt("MULTIPOINT ((0 1))")
  )

  dst_narrow <- geoarrow_compute(
    array,
    "cast",
    list(schema = geoarrow_schema_point())
  )
  dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.point")

  expect_identical(wk::as_wkt(dst_narrow), wk::wkt("POINT (0 1)"))

  array_bad <- geoarrow_create_narrow_from_buffers(
    wk::wkt("MULTIPOINT ((0 1), (2 3))")
  )
  expect_error(
    geoarrow_compute(
      array_bad,
      "cast",
      list(schema = geoarrow_schema_point())
    ),
    "Can't write"
  )
})

test_that("geoarrow_compute() can cast linestring examples to linestring", {
  for (name in c("linestring", "linestring_z", "linestring_m", "linestring_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_linestring())
    )
    dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.linestring")
    dst_narrow$schema$children[[1]]$metadata <- list(
      "ARROW:extension:name" = "geoarrow.point"
    )

    expect_identical(
      wk::as_wkb(dst_narrow),
      wk::as_wkb(geoarrow_create_narrow_from_buffers(src_wkt))
    )
  }
})

test_that("geoarrow_compute() can cast (some) multilinestring to linestring", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::wkt("MULTILINESTRING ((0 1, 2 3))")
  )

  dst_narrow <- geoarrow_compute(
    array,
    "cast",
    list(schema = geoarrow_schema_linestring())
  )
  dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.linestring")
  dst_narrow$schema$children[[1]]$metadata <- list(
    "ARROW:extension:name" = "geoarrow.point"
  )

  expect_identical(
    wk::as_wkt(dst_narrow),
    wk::wkt("LINESTRING (0 1, 2 3)")
  )

  array_bad <- geoarrow_create_narrow_from_buffers(
    wk::wkt("MULTILINESTRING ((0 1, 2 3), (2 3, 4 5))")
  )
  expect_error(
    geoarrow_compute(
      array_bad,
      "cast",
      list(schema = geoarrow_schema_linestring())
    ),
    "Can't write"
  )
})

test_that("geoarrow_compute() can cast polygon examples to polygon", {
  for (name in c("polygon", "polygon_z", "polygon_m", "polygon_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_polygon())
    )
    dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.polygon")
    dst_narrow$schema$children[[1]]$children[[1]]$metadata <- list(
      "ARROW:extension:name" = "geoarrow.point"
    )

    expect_identical(
      wk::as_wkb(dst_narrow),
      wk::as_wkb(geoarrow_create_narrow_from_buffers(src_wkt))
    )
  }
})

test_that("geoarrow_compute() can cast (some) multipolygon to polygon", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::wkt("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))")
  )

  dst_narrow <- geoarrow_compute(
    array,
    "cast",
    list(schema = geoarrow_schema_polygon())
  )
  dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.polygon")
  dst_narrow$schema$children[[1]]$children[[1]]$metadata <- list(
    "ARROW:extension:name" = "geoarrow.point"
  )

  expect_identical(
    wk::as_wkt(dst_narrow),
    wk::wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))")
  )

  array_bad <- geoarrow_create_narrow_from_buffers(
    wk::wkt("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)), ((2 3, 4 5)))")
  )
  expect_error(
    geoarrow_compute(
      array_bad,
      "cast",
      list(schema = geoarrow_schema_polygon())
    ),
    "Can't write"
  )
})

test_that("geoarrow_compute() can cast (multi)point examples", {
  for (name in c("multipoint", "multipoint_z", "multipoint_m", "multipoint_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_multipoint())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multipoint()
    )

    expect_identical(
      wk::as_wkb(dst_narrow),
      wk::as_wkb(geoarrow_create_narrow_from_buffers(src_wkt))
    )
  }

  for (name in c("point", "point_z", "point_m", "point_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt, schema = geoarrow_schema_wkt()),
      "cast",
      list(schema = geoarrow_schema_multipoint())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multipoint()
    )

    expected <- gsub("POINT", "MULTIPOINT", as.character(src_wkt))
    expected <- gsub("(", "((", expected, fixed = TRUE)
    expected <- gsub(")", "))", expected, fixed = TRUE)

    expect_identical(
      wk::as_wkb(dst_narrow),
      wk::as_wkb(expected)
    )
  }
})

test_that("geoarrow_compute() can cast (multi)linestring examples", {
  for (name in c("multilinestring", "multilinestring_z", "multilinestring_m", "multilinestring_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_multilinestring())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multilinestring()
    )

    expect_identical(
      wk::as_wkt(dst_narrow),
      wk::as_wkt(geoarrow_create_narrow_from_buffers(src_wkt))
    )
  }

  for (name in c("linestring", "linestring_z", "linestring_m", "linestring_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_multilinestring())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multilinestring()
    )

    expected <- gsub("LINESTRING", "MULTILINESTRING", as.character(src_wkt))
    expected <- gsub("(", "((", expected, fixed = TRUE)
    expected <- gsub(")", "))", expected, fixed = TRUE)

    expect_identical(
      wk::as_wkt(dst_narrow),
      wk::as_wkt(expected)
    )
  }
})

test_that("geoarrow_compute() can cast (multi)polygon examples", {
  for (name in c("multipolygon", "multipolygon_z", "multipolygon_m", "multipolygon_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_multipolygon())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multipolygon()
    )

    expect_identical(
      wk::as_wkt(dst_narrow),
      wk::as_wkt(geoarrow_create_narrow_from_buffers(src_wkt))
    )
  }

  for (name in c("polygon", "polygon_z", "polygon_m", "polygon_zm")) {
    src_wkt <- geoarrow_example_wkt[[name]]
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(src_wkt),
      "cast",
      list(schema = geoarrow_schema_multipolygon())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multipolygon()
    )

    expected <- gsub("POLYGON", "MULTIPOLYGON", as.character(src_wkt))
    expected <- gsub("([NZM]) \\(", "\\1 \\(\\(", expected)
    expected <- gsub("\\)$", "))", expected)

    expect_identical(
      wk::as_wkt(dst_narrow),
      wk::as_wkt(expected)
    )
  }
})

test_that("geoarrow_compute() can cast all null/empty examples", {
  for (name in names(geoarrow_example_wkt)) {
    src_wkt <- geoarrow_example_wkt[[name]]
    meta <- wk::wk_meta(src_wkt)
    is_null_or_empty <- is.na(meta$geometry_type) | meta$size == 0
    is_null_or_empty[is.na(is_null_or_empty)] <- FALSE

    src_wkt <- src_wkt[is_null_or_empty]
    if (length(src_wkt) == 0) {
      next
    }

    # PointArrayBuilder
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(
        src_wkt,
        schema = geoarrow_schema_wkt()
      ),
      "cast",
      list(schema = geoarrow_schema_point())
    )
    dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.point")

    dst_wkt <- wk::as_wkt(dst_narrow)
    expect_identical(is.na(dst_wkt), is.na(src_wkt))

    # LinestringArrayBuilder
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(
        src_wkt,
        schema = geoarrow_schema_wkt()
      ),
      "cast",
      list(schema = geoarrow_schema_linestring())
    )
    dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.linestring")
    dst_narrow$schema$children[[1]]$metadata <- list(
      "ARROW:extension:name" = "geoarrow.point"
    )

    dst_wkt <- wk::as_wkt(dst_narrow)
    expect_identical(is.na(dst_wkt), is.na(src_wkt))

    # PolygonArrayBuilder
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(
        src_wkt,
        schema = geoarrow_schema_wkt()
      ),
      "cast",
      list(schema = geoarrow_schema_polygon())
    )
    dst_narrow$schema$metadata <- list("ARROW:extension:name" = "geoarrow.polygon")
    dst_narrow$schema$children[[1]]$children[[1]]$metadata <- list(
      "ARROW:extension:name" = "geoarrow.point"
    )

    dst_wkt <- wk::as_wkt(dst_narrow)
    expect_identical(is.na(dst_wkt), is.na(src_wkt))

    # MultiPointArrayBuilder
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(
        src_wkt,
        schema = geoarrow_schema_wkt()
      ),
      "cast",
      list(schema = geoarrow_schema_multipoint())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multipoint()
    )

    dst_wkt <- wk::as_wkt(dst_narrow)
    expect_identical(is.na(dst_wkt), is.na(src_wkt))

    # MultiLinestringArrayBuilder
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(
        src_wkt,
        schema = geoarrow_schema_wkt()
      ),
      "cast",
      list(schema = geoarrow_schema_multilinestring())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multilinestring()
    )

    dst_wkt <- wk::as_wkt(dst_narrow)
    expect_identical(is.na(dst_wkt), is.na(src_wkt))

    # MultiPolygonArrayBuilder
    dst_narrow <- geoarrow_compute(
      geoarrow_create_narrow_from_buffers(
        src_wkt,
        schema = geoarrow_schema_wkt()
      ),
      "cast",
      list(schema = geoarrow_schema_multipolygon())
    )
    dst_narrow$schema <- geoarrow_copy_metadata(
      dst_narrow$schema,
      geoarrow_schema_multipolygon()
    )

    dst_wkt <- wk::as_wkt(dst_narrow)
    expect_identical(is.na(dst_wkt), is.na(src_wkt))
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
