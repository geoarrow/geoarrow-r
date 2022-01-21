
test_that("geoarrow_create() works for geoarrow::geojson", {
  array <- geoarrow::geoarrow_create(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_geojson()
  )

  expect_identical(
    narrow::from_narrow_array(array),
    c(
      "{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}",
      "{\"type\":\"Point\",\"coordinates\":[2.0,2.0]}"
    )
  )
})

test_that("geoarrow_create() works for geoarrow::wkt", {
  array <- geoarrow::geoarrow_create(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(
    narrow::from_narrow_array(array),
    c("POINT (1 1)", "POINT (2 2)")
  )
})

test_that("geoarrow_create() works for geoarrow::wkb", {
  array <- geoarrow::geoarrow_create(
    wk::xy(1:2, 1:2),
    schema = geoarrow_schema_wkb(format = "z"),
    strict = TRUE
  )

  expect_identical(
    array$array_data$buffers[[3]],
    unlist(wk::as_wkb(c("POINT (1 1)", "POINT (2 2)")))
  )
})

test_that("geoarrow_create() works for geoarrow::point", {
  array <- geoarrow::geoarrow_create(
    wk::xy(1:5, 1:5),
    schema = geoarrow_schema_point_struct()
  )

  expect_identical(
    narrow::from_narrow_array(array),
    data.frame(x = as.double(1:5), y = as.double(1:5))
  )
})

test_that("geoarrow_create() works for geoarrow::linestring", {
  array <- geoarrow::geoarrow_create(
    wk::wkt(c("LINESTRING (0 1, 2 3)", "LINESTRING (4 5, 6 7, 8 9)")),
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point_struct()
    )
  )

  expect_identical(
    array$array_data$buffers[[2]],
    c(0L, 2L, 5L)
  )
})

test_that("geoarrow_create() works for geoarrow::polygon", {
  poly <- c(
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10))"
  )

  array <- geoarrow::geoarrow_create(
    wk::wkt(poly),
    schema = geoarrow_schema_polygon(
      point = geoarrow_schema_point_struct()
    )
  )

  expect_null(array$array_data$buffers[[1]])
  expect_identical(as.numeric(array$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(as.numeric(array$array_data$children[[1]]$buffers[[2]]), c(0, 5, 9, 14))
})

test_that("geoarrow_create() works for geoarrow::multi / geoarrow::point", {
  array <- geoarrow::geoarrow_create(
    wk::wkt(c("MULTIPOINT (0 1, 2 3)", "MULTIPOINT (4 5, 6 7, 8 9)")),
    schema = geoarrow_schema_multi(
      geoarrow_schema_point_struct()
    )
  )

  expect_identical(
    array$array_data$buffers[[2]],
    c(0L, 2L, 5L)
  )
})


test_that("geoarrow_create() works for geoarrow::multi / geoarrow::linestring", {
  array <- geoarrow::geoarrow_create(
    wk::wkt(
      c("MULTILINESTRING ((0 1, 2 3))",
        "MULTILINESTRING ((4 5, 6 7, 8 9), (10 11, 12 13))")
    ),
    schema = geoarrow_schema_multi(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point_struct()
      )
    )
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

test_that("geoarrow_create() works for goearrow::multi / geoarrow::polygon", {
  poly_text <- c(
    "MULTIPOLYGON (((35 10, 45 45, 15 40, 10 20, 35 10)), ((20 30, 35 35, 30 20, 20 30)))",
    "MULTIPOLYGON (((35 10, 45 45, 15 40, 10 20, 35 10)))"
  )

  poly <- geoarrow::geoarrow_create(
    wk::wkt(poly_text),
    schema = geoarrow_schema_multi(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point_struct()
      )
    ),
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

test_that("geoarrow_create() errors for invalid schemas", {
  expect_error(
    geoarrow_create(wk::xy(), schema = narrow::narrow_schema("i")),
    "is not TRUE"
  )
  expect_error(
    geoarrow_create(
      wk::xy(),
      schema = narrow::narrow_schema(
        "i",
        metadata = list("ARROW:extension:name" = "not an extension")
      )
    ),
    "Extension 'not an extension' not supported by geoarrow_create"
  )
  expect_error(
    geoarrow_create(
      wk::xy(),
      schema = geoarrow_schema_multi(
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

test_that("geojson arrays can be created", {
  geojson_text <- c(
    "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}",
    "{\"type\":\"LineString\",\"coordinates\":[[1.0,2.0],[3.0,4.0]]}"
  )
  array <- geoarrow_create_geojson_array(
    geojson_text,
    schema = geoarrow_schema_geojson()
  )
  expect_identical(
    narrow::from_narrow_array(array),
    geojson_text
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

  skip_if_not_installed("arrow")
  array_huge_arrow <- narrow::from_narrow_array(array_huge, arrow::Array)
  expect_true(array_huge_arrow$type == arrow::large_binary())
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

  skip_if_not_installed("arrow")

  array_arrow <- narrow::from_narrow_array(array, arrow::Array)
  expect_identical(
    lapply(as.vector(array_arrow), as.raw),
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

  skip_if_not_installed("arrow")

  array_arrow <- narrow::from_narrow_array(array, arrow::Array)
  expect_identical(
    lapply(as.vector(array_arrow), as.raw),
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

  skip_if_not_installed("arrow")

  array_arrow <- narrow::from_narrow_array(array, arrow::Array)
  expect_identical(
    lapply(as.vector(array_arrow), as.raw),
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

  skip_if_not_installed("arrow")
  not_big_array_arrow <- narrow::from_narrow_array(not_big_array, arrow::Array)
  expect_identical(as.vector(not_big_array_arrow), "a totally normal size of string")
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
    geoarrow_schema_multi(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point_struct()
      )
    ),
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

  skip_if_not_installed("arrow")

  poly_arrow <- narrow::from_narrow_array(poly, arrow::Array)
  expect_equal(
    lapply(as.vector(poly_arrow), lapply, lapply, as.data.frame),
    list(
      list(
        list(
          poly_coords[1:5, c("x", "y")]
        ),
        list(
          poly_coords[6:9, c("x", "y")]
        )
      ),
      list(
        list(
          poly_coords[10:14, c("x", "y")]
        )
      )
    ),
    ignore_attr = TRUE
  )
})

test_that("multilinestrings can be created", {
  multi <- geoarrow_create_multilinestring_array(
    wk::xy(1:10, 11:20),
    list(c(2, 1), c(2, 3, 5)),
    geoarrow_schema_multi(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point_struct()
      )
    )
  )
  expect_null(multi$array_data$buffers[[1]])
  expect_identical(as.numeric(multi$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(as.numeric(multi$array_data$children[[1]]$buffers[[2]]), c(0, 2, 5, 10))

  skip_if_not_installed("arrow")

  multi_arrow <- narrow::from_narrow_array(multi, arrow::Array)
  expect_identical(
    lapply(as.vector(multi_arrow), lapply, as.data.frame),
    list(
      list(
        data.frame(x = as.numeric(1:2), y = as.numeric(11:12)),
        data.frame(x = as.numeric(3:5), y = as.numeric(13:15))
      ),
      list(
        data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
      )
    )
  )
})

test_that("multipoints can be created", {
  multi <- geoarrow_create_multipoint_array(
    wk::xy(1:10, 11:20),
    c(5, 5),
    geoarrow_schema_multi(geoarrow_schema_point_struct()),
    strict = TRUE
  )
  expect_null(multi$array_data$buffers[[1]])
  expect_identical(as.numeric(multi$array_data$buffers[[2]]), c(0, 5, 10))

  skip_if_not_installed("arrow")

  multi_arrow <- narrow::from_narrow_array(multi, arrow::Array)
  expect_identical(
    lapply(as.vector(multi_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
    )
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
    geoarrow_schema_polygon(
      point = geoarrow_schema_point_struct()
    )
  )
  expect_null(poly$array_data$buffers[[1]])
  expect_identical(as.numeric(poly$array_data$buffers[[2]]), c(0, 2, 3))
  expect_identical(as.numeric(poly$array_data$children[[1]]$buffers[[2]]), c(0, 5, 9, 14))

  skip_if_not_installed("arrow")

  poly_arrow <- narrow::from_narrow_array(poly, arrow::Array)
  expect_equal(
    lapply(as.vector(poly_arrow), lapply, as.data.frame),
    list(
      list(
        poly_coords[1:5, c("x", "y")],
        poly_coords[6:9, c("x", "y")]
      ),
      list(
        poly_coords[10:14, c("x", "y")]
      )
    ),
    ignore_attr = TRUE
  )
})

test_that("linestring arrays can be created", {
  ls_not_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, 0, 5),
    geoarrow_schema_linestring(
      point = geoarrow_schema_point_struct()
    )
  )
  expect_null(ls_not_null$array_data$buffers[[1]])
  expect_identical(as.numeric(ls_not_null$array_data$buffers[[2]]), c(0, 5, 5, 10))

  ls_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, NA, 5),
    geoarrow_schema_linestring(
      point = geoarrow_schema_point_struct()
    )
  )
  expect_identical(
    as.logical(ls_null$array_data$buffers[[1]])[1:3],
    c(TRUE, FALSE, TRUE)
  )
  expect_identical(as.numeric(ls_null$array_data$buffers[[2]]), c(0, 5, 5, 10))

  skip_if_not_installed("arrow")

  ls_not_null_arrow <- narrow::from_narrow_array(ls_not_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_not_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(x = double(), y = double()),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
    )
  )

  ls_null_arrow <- narrow::from_narrow_array(ls_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
    )
  )
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
    geoarrow_schema_point()
  )

  expect_identical(
    as.logical(array_null$array_data$buffers[[1]])[1:length(coords)],
    c(TRUE, TRUE, FALSE)
  )

  skip_if_not_installed("arrow")

  array_not_null_arrow <- narrow::from_narrow_array(array_not_null, arrow::Array)
  expect_identical(
    lapply(as.vector(array_not_null_arrow), as.numeric),
    list(c(1, 3), c(2, 4))
  )

  array_null_arrow <- narrow::from_narrow_array(array_null, arrow::Array)
  expect_identical(
    lapply(as.vector(array_null_arrow), as.numeric),
    list(c(1, 3), c(2, 4), double())
  )
})

test_that("point struct arrays can be created", {
  coords <- wk::xy(c(1:2, NA), c(3:4, NA))

  array_not_null <- geoarrow_create_point_array(
    coords[1:2],
    geoarrow_schema_point_struct()
  )
  expect_null(array_not_null$array_data$buffers[[1]])

  array_null <- geoarrow_create_point_array(
    coords,
    geoarrow_schema_point_struct()
  )

  expect_identical(
    as.logical(array_null$array_data$buffers[[1]])[1:length(coords)],
    c(TRUE, TRUE, FALSE)
  )
})

test_that("point struct arrays can be created for all dimensions", {
  coords_xy <- wk::xy(1:100, 101:200)
  array_xy <- geoarrow_create_point_array(coords_xy, geoarrow_schema_point_struct(dim = "xy"))
  expect_identical(
    narrow::from_narrow_array(array_xy),
    data.frame(x = as.double(1:100), y = as.double(101:200))
  )

  coords_xyz <- wk::xyz(1:100, 101:200, 201:300)
  array_xyz <- geoarrow_create_point_array(coords_xyz, geoarrow_schema_point_struct(dim = "xyz"))
  expect_identical(
    narrow::from_narrow_array(array_xyz),
    data.frame(x = as.double(1:100), y = as.double(101:200), z = as.double(201:300))
  )

  coords_xym <- wk::xym(1:100, 101:200, 301:400)
  array_xym <- geoarrow_create_point_array(coords_xym, geoarrow_schema_point_struct(dim = "xym"))
  expect_identical(
    narrow::from_narrow_array(array_xym),
    data.frame(x = as.double(1:100), y = as.double(101:200), m = as.double(301:400))
  )

  coords_xyzm <- wk::xyzm(1:100, 101:200, 201:300, 301:400)
  array_xyzm <- geoarrow_create_point_array(coords_xyzm, geoarrow_schema_point_struct(dim = "xyzm"))
  expect_identical(
    narrow::from_narrow_array(array_xyzm),
    data.frame(
      x = as.double(1:100), y = as.double(101:200),
      z = as.double(201:300), m = as.double(301:400)
    )
  )

  # check that these round trip to Arrow
  skip_if_not_installed("arrow")

  array_xy_arrow <- narrow::from_narrow_array(array_xy, arrow::Array)
  expect_identical(
    as.vector(array_xy_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200)
        )
      )
    )
  )

  array_xyz_arrow <- narrow::from_narrow_array(array_xyz, arrow::Array)
  expect_identical(
    as.vector(array_xyz_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200),
          z = as.numeric(201:300)
        )
      )
    )
  )

  array_xym_arrow <- narrow::from_narrow_array(array_xym, arrow::Array)
  expect_identical(
    as.vector(array_xym_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200),
          m = as.numeric(301:400)
        )
      )
    )
  )

  array_xyzm_arrow <- narrow::from_narrow_array(array_xyzm, arrow::Array)
  expect_identical(
    as.vector(array_xyzm_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200),
          z = as.numeric(201:300),
          m = as.numeric(301:400)
        )
      )
    )
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
          "ARROW:extension:metadata" = geoarrow_metadata_serialize(dim = "fish")
        ))
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
          "ARROW:extension:metadata" = geoarrow_metadata_serialize(dim = "xy")
        ))
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
    "geoarrow.multi"
  )
  expect_identical(
    schema_multipoint$children[[1]]$metadata[["ARROW:extension:name"]],
    "geoarrow.point"
  )

  schema_multilinestring <- geoarrow_schema_default(wk::wkt("MULTILINESTRING ((1 1, 2 2))"))
  expect_identical(
    schema_multilinestring$metadata[["ARROW:extension:name"]],
    "geoarrow.multi"
  )
  expect_identical(
    schema_multilinestring$children[[1]]$metadata[["ARROW:extension:name"]],
    "geoarrow.linestring"
  )

  schema_multipolygon <- geoarrow_schema_default(wk::wkt("MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)))"))
  expect_identical(
    schema_multipolygon$metadata[["ARROW:extension:name"]],
    "geoarrow.multi"
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
  geoarrow_meta <- geoarrow_metadata(schema_point)
  expect_identical(geoarrow_meta$dim, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::xyz(1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyz)
  expect_identical(geoarrow_meta$dim, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::xym(1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xym)
  expect_identical(geoarrow_meta$dim, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::xyzm(1:2, 1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyzm)
  expect_identical(geoarrow_meta$dim, "xyzm")
})

test_that("geoarrow_schema_default() detects dimensions from meta", {
  schema_point <- geoarrow_schema_default(wk::as_wkt(wk::xy(1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point)
  expect_identical(geoarrow_meta$dim, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::as_wkt(wk::xyz(1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyz)
  expect_identical(geoarrow_meta$dim, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::as_wkt(wk::xym(1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xym)
  expect_identical(geoarrow_meta$dim, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::as_wkt(wk::xyzm(1:2, 1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyzm)
  expect_identical(geoarrow_meta$dim, "xyzm")
})
