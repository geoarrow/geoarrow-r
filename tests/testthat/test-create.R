
test_that("geoarrow_create_narrow() can use geoarrow_compute() for WKT", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::xy(1:2, 1:2)),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(
    as.character(narrow::from_narrow_array(array)),
    c("POINT (1 1)", "POINT (2 2)")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for WKB", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::xy(1:2, 1:2)),
    schema = geoarrow_schema_wkb()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for point", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::xy(1:2, 1:2)),
    schema = geoarrow_schema_point()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for linestring", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::wkt("LINESTRING (0 1, 2 3)")),
    schema = geoarrow_schema_linestring()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("LINESTRING (0 1, 2 3)")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for polygon", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))")),
    schema = geoarrow_schema_polygon()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for multipoint", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::wkt("MULTIPOINT ((0 0), (1 0), (0 1), (0 0))")),
    schema = geoarrow_schema_multipoint()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTIPOINT ((0 0), (1 0), (0 1), (0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for multilinestring", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::wkt("MULTILINESTRING ((0 0, 1 0, 0 1, 0 0))")),
    schema = geoarrow_schema_multilinestring()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTILINESTRING ((0 0, 1 0, 0 1, 0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for multipolygon", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow_from_buffers(wk::wkt("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))")),
    schema = geoarrow_schema_multipolygon()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for WKT", {
  array <- geoarrow_create_narrow(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(
    as.character(narrow::from_narrow_array(array)),
    c("POINT (1 1)", "POINT (2 2)")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for WKB", {
  array <- geoarrow_create_narrow(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_wkb()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for point", {
  array <- geoarrow_create_narrow(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_point()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for linestring", {
  array <- geoarrow_create_narrow(
    wk::wkt("LINESTRING (0 1, 2 3)"),
    schema = geoarrow_schema_linestring()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("LINESTRING (0 1, 2 3)")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for polygon", {
  array <- geoarrow_create_narrow(
    wk::wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
    schema = geoarrow_schema_polygon()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for multipoint", {
  array <- geoarrow_create_narrow(
    wk::wkt("MULTIPOINT ((0 0), (1 0), (0 1), (0 0))"),
    schema = geoarrow_schema_multipoint()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTIPOINT ((0 0), (1 0), (0 1), (0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for multilinestring", {
  array <- geoarrow_create_narrow(
    wk::wkt("MULTILINESTRING ((0 0, 1 0, 0 1, 0 0))"),
    schema = geoarrow_schema_multilinestring()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTILINESTRING ((0 0, 1 0, 0 1, 0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute_handler() for multipolygon", {
  array <- geoarrow_create_narrow(
    wk::wkt("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))"),
    schema = geoarrow_schema_multipolygon()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))")
  )
})

test_that("geoarrow_create_narrow() propagates CRS/geodesic", {
  array <- geoarrow_create_narrow(
    wk::xy(1:2, 1:2, crs = "EPSG:1234"),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(wk::wk_crs(array), "EPSG:1234")

  array <- geoarrow_create_narrow(
    wk::wkt("LINESTRING (0 0, 1 1)", geodesic = TRUE),
    schema = geoarrow_schema_wkt()
  )

  expect_true(wk::wk_is_geodesic(array))
})

test_that("goearrow_create_narrow() falls back to geoarrow_create_narrow_from_buffers()", {
  array <- geoarrow_create_narrow(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_wkb(),
    strict = TRUE
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow_from_buffers() works for geoarrow::wkt", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(
    narrow::from_narrow_array(array),
    c("POINT (1 1)", "POINT (2 2)")
  )
})

test_that("geoarrow_create_narrow_from_buffers() works for geoarrow::wkb", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_wkb(format = "z"),
    strict = TRUE
  )

  expect_identical(
    array$array_data$buffers[[3]],
    unlist(wk::as_wkb(c("POINT (1 1)", "POINT (2 2)")))
  )
})

test_that("geoarrow_create_narrow_from_buffers() works for geoarrow::point", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::xy(1:5, 1:5),
    schema = geoarrow_schema_point()
  )

  expect_equal(
    array$array_data$children[[1]]$buffers[[2]],
    rep(c(1:5), each = 2)
  )
})

test_that("geoarrow_create_narrow_from_buffers() works for geoarrow::linestring", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::wkt(c("LINESTRING (0 1, 2 3)", "LINESTRING (4 5, 6 7, 8 9)")),
    schema = geoarrow_schema_linestring()
  )

  expect_identical(
    array$array_data$buffers[[2]],
    c(0L, 2L, 5L)
  )
})

test_that("geoarrow_create_narrow_from_buffers() works for geoarrow::polygon", {
  poly <- c(
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10))"
  )

  array <- geoarrow_create_narrow_from_buffers(
    wk::wkt(poly),
    schema = geoarrow_schema_polygon()
  )

  expect_null(array$array_data$buffers[[1]])
  expect_identical(as.numeric(array$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(as.numeric(array$array_data$children[[1]]$buffers[[2]]), c(0, 5, 9, 14))
})

test_that("geoarrow_create_narrow_from_buffers() works for geoarrow::multi / geoarrow::point", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::wkt(c("MULTIPOINT (0 1, 2 3)", "MULTIPOINT (4 5, 6 7, 8 9)")),
    schema = geoarrow_schema_multipoint()
  )

  expect_identical(
    array$array_data$buffers[[2]],
    c(0L, 2L, 5L)
  )
})


test_that("geoarrow_create_narrow_from_buffers() works for geoarrow::multi / geoarrow::linestring", {
  array <- geoarrow_create_narrow_from_buffers(
    wk::wkt(
      c("MULTILINESTRING ((0 1, 2 3))",
        "MULTILINESTRING ((4 5, 6 7, 8 9), (10 11, 12 13))")
    ),
    schema = geoarrow_schema_multilinestring()
  )

  expect_identical(
    array$array_data$buffers[[2]],
    c(0L, 1L, 3L)
  )

  expect_identical(
    array$array_data$children[[1]]$buffers[[2]],
    c(0L, 2L, 5L, 7L)
  )
})

test_that("geoarrow_create_narrow_from_buffers() works for goearrow::multi / geoarrow::polygon", {
  poly_text <- c(
    "MULTIPOLYGON (((35 10, 45 45, 15 40, 10 20, 35 10)), ((20 30, 35 35, 30 20, 20 30)))",
    "MULTIPOLYGON (((35 10, 45 45, 15 40, 10 20, 35 10)))"
  )

  poly <- geoarrow_create_narrow_from_buffers(
    wk::wkt(poly_text),
    schema = geoarrow_schema_multipolygon(),
    strict = TRUE
  )
  expect_null(poly$array_data$buffers[[1]])
  expect_identical(as.numeric(poly$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(
    as.numeric(poly$array_data$children[[1]]$buffers[[2]]),
    c(0, 1, 2, 3)
  )
  expect_identical(
    as.numeric(poly$array_data$children[[1]]$children[[1]]$buffers[[2]]),
    c(0, 5, 9, 14)
  )
})


test_that("geoarrow_create_narrow_from_buffers() works for empty geometries", {
  array_point <- geoarrow_create_narrow_from_buffers(wk::wkt("POINT EMPTY"))
  expect_identical(wk::as_wkt(array_point), wk::wkt("POINT (nan nan)"))

  empties <- wk::wkt(
    c(
      "LINESTRING EMPTY", "POLYGON EMPTY",
      "MULTIPOINT EMPTY", "MULTILINESTRING EMPTY",
      "MULTIPOLYGON EMPTY"
    )
  )

  for (i in seq_along(empties)) {
    expect_identical(
      wk::as_wkt(geoarrow_create_narrow_from_buffers(!! empties[i])),
      !! empties[i]
    )
  }
})

test_that("geoarrow_create_narrow_from_buffers() errors for invalid schemas", {
  expect_error(
    geoarrow_create_narrow_from_buffers(wk::xy(), schema = narrow::narrow_schema("i")),
    "is not TRUE"
  )
  expect_error(
    geoarrow_create_narrow_from_buffers(
      wk::xy(),
      schema = narrow::narrow_schema(
        "i",
        metadata = list("ARROW:extension:name" = "not an extension")
      )
    ),
    "Extension 'not an extension' not supported by geoarrow_create_narrow"
  )
  expect_error(
    geoarrow_create_narrow_from_buffers(
      wk::xy(),
      schema = geoarrow_schema_collection(
        narrow::narrow_schema(
          "i",
          metadata = list("ARROW:extension:name" = "not an extension")
        )
      )
    ),
    "Extension 'not an extension' not supported within 'geoarrow::multi'"
  )
})

test_that("wkt arrays can be created", {
  array <- geoarrow_create_wkt_array(
    c("POINT (1 2)", "LINESTRING (1 2, 3 4)"),
    schema = geoarrow_schema_wkt()
  )
  expect_identical(
    narrow::from_narrow_array(array),
    c("POINT (1 2)", "LINESTRING (1 2, 3 4)")
  )
})

test_that("WKB arrays auto-set format based on data", {
  array_fixed_width <- geoarrow_create_wkb_array(
    unclass(wk::as_wkb(c("POINT (1 2)", "POINT (3 4)"))),
    schema = geoarrow_schema_wkb(format = "z"),
    strict = FALSE
  )
  expect_identical(array_fixed_width$schema$format, "w:21")

  skip_if_not(identical(Sys.getenv("ARROW_LARGE_MEMORY_TESTS"), "true"))

  # invalid WKB but fast to generate
  array_huge <- geoarrow_create_wkb_array(
    list(raw(1), raw(2 ^ 31 - 1)),
    schema = geoarrow_schema_wkb(format = "z"),
    strict = FALSE
  )
  expect_identical(array_huge$schema$format, "Z")

  skip_if_not(has_arrow_with_extension_type())
  array_huge_arrow <- narrow::from_narrow_array(array_huge, arrow::Array)
  expect_true(array_huge_arrow$type$storage_type() == arrow::large_binary())
})

test_that("WKB arrays can be created", {
  wkb_list <- unclass(wk::as_wkb(c("POINT (1 2)", "LINESTRING (1 2, 3 4)", NA)))
  array <- geoarrow_create_wkb_array(
    wkb_list,
    schema = geoarrow_schema_wkb(format = "z")
  )

  expect_identical(as.logical(array$array_data$buffers[[1]])[1:3], c(TRUE, TRUE, FALSE))
  expect_identical(diff(array$array_data$buffers[[2]]), as.integer(lengths(wkb_list)))
  expect_identical(array$array_data$buffers[[3]], unlist(wkb_list))

  skip_if_not(has_arrow_with_extension_type())

  array_arrow <- narrow::from_narrow_array(array, arrow::Array)
  expect_identical(
    lapply(as.vector(array_arrow$storage()), as.raw),
    lapply(wkb_list, as.raw)
  )
})

test_that("large WKB arrays can be created", {
  wkb_list <- unclass(wk::as_wkb(c("POINT (1 2)", "LINESTRING (1 2, 3 4)", NA)))
  array <- geoarrow_create_wkb_array(
    wkb_list,
    schema = geoarrow_schema_wkb(format = "Z")
  )

  expect_identical(as.logical(array$array_data$buffers[[1]])[1:3], c(TRUE, TRUE, FALSE))
  expect_identical(diff(as.integer(array$array_data$buffers[[2]])), as.integer(lengths(wkb_list)))
  expect_identical(array$array_data$buffers[[3]], unlist(wkb_list))

  skip_if_not(has_arrow_with_extension_type())

  array_arrow <- narrow::from_narrow_array(array, arrow::Array)
  expect_identical(
    lapply(as.vector(array_arrow$storage()), as.raw),
    lapply(wkb_list, as.raw)
  )
})

test_that("fixed-width WKB arrays can be created", {
  wkb_list <- unclass(wk::as_wkb(c("POINT (1 2)", "POINT (2 3)")))
  array <- geoarrow_create_wkb_array(
    wkb_list,
    schema = geoarrow_schema_wkb(format = "w:21")
  )

  expect_identical(array$array_data$buffers[[2]], unlist(wkb_list))

  skip_if_not(has_arrow_with_extension_type())

  array_arrow <- narrow::from_narrow_array(array, arrow::Array)
  expect_identical(
    lapply(as.vector(array_arrow$storage()), as.raw),
    wkb_list
  )
})

test_that("WKB creation errors with invalid schema", {
  expect_error(
    geoarrow_create_wkb_array(
      list(raw()),
      schema = narrow::narrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow.wkb"
        )
      ),
      strict = TRUE
    ),
    "Unsupported binary encoding format"
  )
})

test_that("geoarrow_create_string_array() respects strict = TRUE", {
  # make sure output is large if requested
  not_big_array <- geoarrow_create_string_array(
    "a totally normal size of string",
    schema = geoarrow_schema_wkt(format = "U"),
    strict = TRUE
  )
  expect_identical(
    narrow::from_narrow_array(not_big_array),
    "a totally normal size of string"
  )

  skip_if_not(identical(Sys.getenv("ARROW_LARGE_MEMORY_TESTS"), "true"))

  long_text <- c("a", strrep("b", 2 ^ 31 - 1))
  expect_error(
    geoarrow_create_string_array(
      long_text,
      schema = geoarrow_schema_wkt(format = "u"),
      strict = TRUE
    ),
    "Attempt to create small unicode array with"
  )

  skip_if_not(has_arrow_with_extension_type())
  not_big_array_arrow <- narrow::from_narrow_array(not_big_array, arrow::Array)
  expect_identical(
    as.vector(not_big_array_arrow$storage()),
    "a totally normal size of string"
  )
})

test_that("multipolygons can be created", {
  poly <- c(
    "MULTIPOLYGON (((35 10, 45 45, 15 40, 10 20, 35 10)), ((20 30, 35 35, 30 20, 20 30)))",
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10))"
  )
  poly_coords <- wk::wk_coords(wk::as_wkt(poly))

  poly <- geoarrow_create_multipolygon_array(
    wk::xy(poly_coords$x, poly_coords$y),
    list(c(2, 1), c(1, 1, 1), c(5, 4, 5)),
    geoarrow_schema_multipolygon(),
    strict = TRUE
  )
  expect_null(poly$array_data$buffers[[1]])
  expect_identical(as.numeric(poly$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(
    as.numeric(poly$array_data$children[[1]]$buffers[[2]]),
    c(0, 1, 2, 3)
  )
  expect_identical(
    as.numeric(poly$array_data$children[[1]]$children[[1]]$buffers[[2]]),
    c(0, 5, 9, 14)
  )

  skip_if_not(has_arrow_with_extension_type())

  poly_arrow <- narrow::from_narrow_array(poly, arrow::Array)
  expect_identical(
    poly_arrow$type$extension_name(),
    "geoarrow.multipolygon"
  )
})

test_that("multilinestrings can be created", {
  multi <- geoarrow_create_multilinestring_array(
    wk::xy(1:10, 11:20),
    list(c(2, 1), c(2, 3, 5)),
    geoarrow_schema_multilinestring()
  )
  expect_null(multi$array_data$buffers[[1]])
  expect_identical(as.numeric(multi$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(as.numeric(multi$array_data$children[[1]]$buffers[[2]]), c(0, 2, 5, 10))

  skip_if_not(has_arrow_with_extension_type())

  multi_arrow <- narrow::from_narrow_array(
    strip_extensions(multi),
    arrow::Array
  )
})

test_that("multipoints can be created", {
  multi <- geoarrow_create_multipoint_array(
    wk::xy(1:10, 11:20),
    c(5, 5),
    geoarrow_schema_multipoint(),
    strict = TRUE
  )
  expect_null(multi$array_data$buffers[[1]])
  expect_identical(as.numeric(multi$array_data$buffers[[2]]), c(0, 5, 10))

  skip_if_not(has_arrow_with_extension_type())

  multi_arrow <- narrow::from_narrow_array(
    strip_extensions(multi),
    arrow::Array
  )
})

test_that("polygons can be created", {
  poly <- c(
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10))"
  )
  poly_coords <- wk::wk_coords(wk::as_wkt(poly))

  poly <- geoarrow_create_polygon_array(
    wk::xy(poly_coords$x, poly_coords$y),
    list(c(2, 1), c(5, 4, 5)),
    geoarrow_schema_polygon()
  )
  expect_null(poly$array_data$buffers[[1]])
  expect_identical(as.numeric(poly$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(as.numeric(poly$array_data$children[[1]]$buffers[[2]]), c(0, 5, 9, 14))

  skip_if_not(has_arrow_with_extension_type())
})

test_that("linestring arrays can be created", {
  ls_not_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, 0, 5),
    geoarrow_schema_linestring()
  )
  expect_null(ls_not_null$array_data$buffers[[1]])
  expect_identical(as.numeric(ls_not_null$array_data$buffers[[2]]), c(0, 5, 5, 10))

  ls_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, NA, 5),
    geoarrow_schema_linestring()
  )
  expect_identical(
    as.logical(ls_null$array_data$buffers[[1]])[1:3],
    c(TRUE, FALSE, TRUE)
  )
  expect_identical(as.numeric(ls_null$array_data$buffers[[2]]), c(0, 5, 5, 10))
})

test_that("linestring arrays error for invalid schemas", {
  expect_error(
    geoarrow_create_linestring_array(wk::xy(), integer(), narrow::narrow_schema("i")),
    "geoarrow.linestring"
  )

  expect_error(
    geoarrow_create_linestring_array(
      wk::xy(),
      integer(),
      narrow::narrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow.linestring",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize()
        ))
    ),
    "Unsupported nested list storage type"
  )
})

test_that("point arrays can be created", {
  coords <- wk::xy(c(1:2, NA), c(3:4, NA))

  array_not_null <- geoarrow_create_point_array(
    coords[1:2],
    geoarrow_schema_point()
  )
  expect_null(array_not_null$array_data$buffers[[1]])

  array_null <- geoarrow_create_point_array(
    coords,
    geoarrow_schema_point(),
    can_be_null = NA
  )

  expect_identical(
    as.logical(array_null$array_data$buffers[[1]])[1:length(coords)],
    c(TRUE, TRUE, FALSE)
  )

  skip_if_not(has_arrow_with_extension_type())

  array_not_null_arrow <- narrow::from_narrow_array(
    strip_extensions(array_not_null),
    arrow::Array
  )

  expect_identical(
    lapply(as.vector(array_not_null_arrow), as.numeric),
    list(c(1, 3), c(2, 4))
  )

  array_null_arrow <- narrow::from_narrow_array(
    strip_extensions(array_null),
    arrow::Array
  )

  expect_identical(
    lapply(as.vector(array_null_arrow), as.numeric),
    list(c(1, 3), c(2, 4), double())
  )
})

test_that("point arrays can be created with null_point_as_empty = TRUE and FALSE", {
  features <- wk::wkt(c("POINT (1 3)", "POINT (2 4)", NA))

  array_with_nulls <- geoarrow_create_narrow_from_buffers(
    features,
    schema = geoarrow_schema_point(),
    null_point_as_empty = FALSE
  )
  expect_identical(array_with_nulls$array_data$null_count, 1L)
  expect_identical(
    as.logical(array_with_nulls$array_data$buffers[[1]])[1:length(features)],
    c(TRUE, TRUE, FALSE)
  )

  array_with_empty <- geoarrow_create_narrow_from_buffers(
    features,
    schema = geoarrow_schema_point(),
    null_point_as_empty = TRUE
  )
  expect_identical(array_with_empty$array_data$null_count, 0L)
  expect_null(array_with_empty$array_data$buffers[[1]])
})

test_that("point struct arrays can be created", {
  coords <- wk::xy(c(1:2, NA), c(3:4, NA))

  array_not_null <- geoarrow_create_point_array(
    coords[1:2],
    geoarrow_schema_point()
  )
  expect_null(array_not_null$array_data$buffers[[1]])

  array_null <- geoarrow_create_point_array(
    coords,
    geoarrow_schema_point(),
    can_be_null = NA
  )

  expect_identical(
    as.logical(array_null$array_data$buffers[[1]])[1:length(coords)],
    c(TRUE, TRUE, FALSE)
  )
})

test_that("point arrays can't be created from invalid schemas", {
  expect_error(
    geoarrow_create_point_array(wk::xy(), narrow::narrow_schema("i")),
    "geoarrow.point"
  )

  expect_error(
    geoarrow_create_point_array(
      wk::xy(),
      narrow::narrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow.point",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize()
        ))
    ),
    "dim"
  )

  expect_error(
    geoarrow_create_point_array(
      wk::xy(),
      narrow::narrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow.point",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize()
        ),
        children = list(
          narrow::narrow_schema("g", name = "fish")
        )
      )
    ),
    "xyzm"
  )

  expect_error(
    geoarrow_create_point_array(
      wk::xy(),
      narrow::narrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow.point",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize()
        ),
        children = list(narrow::narrow_schema("i", name = "xy"))
      )
    ),
    "Unsupported point storage type"
  )
})

test_that("geoarrow_schema_default() works with all geometry types", {
  schema_point <- geoarrow_schema_default(wk::xy())
  expect_identical(
    schema_point$metadata[["ARROW:extension:name"]],
    "geoarrow.point"
  )

  schema_linestring <- geoarrow_schema_default(wk::wkt("LINESTRING (1 1, 2 2)"))
  expect_identical(
    schema_linestring$metadata[["ARROW:extension:name"]],
    "geoarrow.linestring"
  )

  schema_polygon <- geoarrow_schema_default(wk::wkt("POLYGON ((0 0, 1 1, 0 1, 0 0))"))
  expect_identical(
    schema_polygon$metadata[["ARROW:extension:name"]],
    "geoarrow.polygon"
  )

  schema_multipoint <- geoarrow_schema_default(wk::wkt("MULTIPOINT (1 1, 2 2)"))
  expect_identical(
    schema_multipoint$metadata[["ARROW:extension:name"]],
    "geoarrow.multipoint"
  )
  expect_identical(
    schema_multipoint$children[[1]]$metadata[["ARROW:extension:name"]],
    "geoarrow.point"
  )

  schema_multilinestring <- geoarrow_schema_default(wk::wkt("MULTILINESTRING ((1 1, 2 2))"))
  expect_identical(
    schema_multilinestring$metadata[["ARROW:extension:name"]],
    "geoarrow.multilinestring"
  )
  expect_identical(
    schema_multilinestring$children[[1]]$metadata[["ARROW:extension:name"]],
    "geoarrow.linestring"
  )

  schema_multipolygon <- geoarrow_schema_default(wk::wkt("MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)))"))
  expect_identical(
    schema_multipolygon$metadata[["ARROW:extension:name"]],
    "geoarrow.multipolygon"
  )
  expect_identical(
    schema_multipolygon$children[[1]]$metadata[["ARROW:extension:name"]],
    "geoarrow.polygon"
  )
})

test_that("geoarrow_schema_default() falls back to WKB for mixed type vectors", {
  schema_collection <- geoarrow_schema_default(wk::wkt("GEOMETRYCOLLECTION (POINT (1 2))"))
  expect_identical(
    schema_collection$metadata[["ARROW:extension:name"]],
    "geoarrow.wkb"
  )

  schema_mixed <- geoarrow_schema_default(wk::wkt(c("POINT (1 2)", "MULTIPOINT (1 2, 3 4)")))
  expect_identical(
    schema_mixed$metadata[["ARROW:extension:name"]],
    "geoarrow.wkb"
  )
})

test_that("geoarrow_schema_default() works with and without wk::wk_vector_meta()", {
  schema_point <- geoarrow_schema_default(wk::xy(1:2, 1:2))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow.point")

  schema_point_crs <- geoarrow_schema_default(wk::xy(1:2, 1:2, crs = "EPSG:3857"))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow.point")
  geoarrow_meta <- geoarrow_metadata(schema_point_crs)
  expect_identical(geoarrow_meta$crs, "EPSG:3857")

  schema_wkt <- geoarrow_schema_default(wk::as_wkt(wk::xy(1:2, 1:2)))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow.point")
})

test_that("geoarrow_schema_default() detects dimensions from vector_meta", {
  schema_point <- geoarrow_schema_default(wk::xy(1:2, 1:2))
  expect_identical(schema_point$children[[1]]$name, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::xyz(1:2, 1:2, 1:2))
  expect_identical(schema_point_xyz$children[[1]]$name, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::xym(1:2, 1:2, 1:2))
  expect_identical(schema_point_xym$children[[1]]$name, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::xyzm(1:2, 1:2, 1:2, 1:2))
  expect_identical(schema_point_xyzm$children[[1]]$name, "xyzm")
})

test_that("geoarrow_schema_default() detects dimensions from meta", {
  schema_point <- geoarrow_schema_default(wk::wkt("POINT (1 1)"))
  expect_identical(schema_point$children[[1]]$name, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::wkt("POINT Z (1 1 1)"))
  expect_identical(schema_point_xyz$children[[1]]$name, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::wkt("POINT M (1 1 1)"))
  expect_identical(schema_point_xym$children[[1]]$name, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::wkt("POINT ZM (1 1 1 1)"))
  expect_identical(schema_point_xyzm$children[[1]]$name, "xyzm")
})
