
test_that("geoparquet_metadata() works", {
  expect_error(geoparquet_metadata(narrow::narrow_schema("i")), "zero columns")

  schema <- narrow::narrow_schema(
    format = "+s",
    children = list(
      geoarrow_schema_wkb(name = "col1"),
      geoarrow_schema_wkt(name = "col2")
    )
  )

  meta <- geoparquet_metadata(schema)
  expect_identical(meta$primary_column, "col1")
  expect_identical(meta$columns$col1$encoding, "WKB")
  expect_identical(meta$columns$col2$encoding, "WKT")

  meta2 <- geoparquet_metadata(schema, primary_column = "col2")
  expect_identical(meta2$primary_column, "col2")
})

test_that("geoparquet_column_metadata() works for flat types", {
  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_wkt()),
    list(crs = geoparquet_crs_if_null(), encoding = "WKT")
  )

  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_wkt(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", encoding = "WKT")
  )

  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_wkt(edges = "spherical")),
    list(crs = geoparquet_crs_if_null(), edges = "spherical", encoding = "WKT")
  )

  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_wkb()),
    list(crs = geoparquet_crs_if_null(), encoding = "WKB")
  )

  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_point(dim = "xyz")),
    list(crs = geoparquet_crs_if_null(), encoding = "geoarrow.point")
  )

  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_point(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", encoding = "geoarrow.point")
  )
})

test_that("geoparquet_column_metadata() works for linestring", {
  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_linestring()),
    list(
      crs = geoparquet_crs_if_null(),
      encoding = "geoarrow.linestring"
    )
  )

  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_linestring(edges = "spherical")),
    list(
      crs = geoparquet_crs_if_null(),
      edges = "spherical",
      encoding = "geoarrow.linestring"
    )
  )

  expect_mapequal(
    geoparquet_column_metadata(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.linestring"
    )
  )
})

test_that("geoparquet_column_metadata() works for polygon", {
  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_polygon()),
    list(
      crs = geoparquet_crs_if_null(),
      encoding = "geoarrow.polygon"
    )
  )

  expect_mapequal(
    geoparquet_column_metadata(geoarrow_schema_polygon(edges = "spherical")),
    list(
      crs = geoparquet_crs_if_null(),
      edges = "spherical",
      encoding = "geoarrow.polygon"
    )
  )

  expect_mapequal(
    geoparquet_column_metadata(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.polygon"
    )
  )
})

test_that("geoparquet_column_metadata() works for polygon", {
  expect_mapequal(
    geoparquet_column_metadata(
      geoarrow_schema_multipoint()
    ),
    list(
      crs = geoparquet_crs_if_null(),
      encoding = "geoarrow.multipoint"
    )
  )

  expect_mapequal(
    geoparquet_column_metadata(
      geoarrow_schema_multilinestring(edges = "spherical")
    ),
    list(
      crs = geoparquet_crs_if_null(),
      edges = "spherical",
      encoding = "geoarrow.multilinestring"
    )
  )

  expect_mapequal(
    geoparquet_column_metadata(
      geoarrow_schema_multipoint(crs = "EPSG:1234")
    ),
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.multipoint"
    )
  )
})

test_that("geoparquet_column_metadata() can include bbox and geometry_type", {
  expect_mapequal(
    geoparquet_column_metadata(
      geoarrow_schema_multipoint(),
      array = geoarrow_create_narrow_from_buffers(
        wk::wkt("MULTIPOINT (0 1, 2 3)")
      )
    ),
    list(
      crs = geoparquet_crs_if_null(),
      encoding = "geoarrow.multipoint",
      bbox = c(0, 1, 2, 3),
      geometry_type = "MultiPoint"
    )
  )
})

test_that("geometry_type column metadata is correct", {
  # works with vector meta
  expect_identical(
    geoparquet_geometry_type(
      geoarrow_create_narrow_from_buffers(wk::wkt("POINT (0 1)"))
    ),
    "Point"
  )

  expect_identical(
    geoparquet_geometry_type(
      geoarrow_create_narrow_from_buffers(wk::wkt("POINT Z (0 1 2)"))
    ),
    "Point Z"
  )

  expect_identical(
    geoparquet_geometry_type(
      geoarrow_create_narrow_from_buffers(wk::wkt("POINT M (0 1 2)"))
    ),
    "Point M"
  )

  expect_identical(
    geoparquet_geometry_type(
      geoarrow_create_narrow_from_buffers(wk::wkt("POINT ZM (0 1 2 3)"))
    ),
    "Point ZM"
  )

  # needs compute
  expect_identical(
    geoparquet_geometry_type(
      geoarrow_create_narrow_from_buffers(
        wk::wkt("POINT (0 1)"),
        schema = geoarrow_schema_wkt()
      )
    ),
    "Point"
  )

  # with multiple types
  expect_setequal(
    geoparquet_geometry_type(
      geoarrow_create_narrow_from_buffers(
        wk::wkt(c("POINT (0 1)", "LINESTRING (0 1, 2 3)")),
        schema = geoarrow_schema_wkt()
      )
    ),
    c("LineString", "Point")
  )
})

test_that("bbox column metadata is correct", {
  expect_identical(
    geoparquet_bbox(
      geoarrow_create_narrow_from_buffers(
        wk::wkt("LINESTRING EMPTY")
      )
    ),
    NULL
  )

  expect_identical(
    geoparquet_bbox(
      geoarrow_create_narrow_from_buffers(
        wk::wkt("LINESTRING (0 1, 2 3)")
      )
    ),
    c(0, 1, 2, 3)
  )

  expect_identical(
    geoparquet_bbox(
      geoarrow_create_narrow_from_buffers(
        wk::wkt("LINESTRING Z (0 1 2, 2 3 4)")
      )
    ),
    c(0, 1, 2, 3)
  )

  expect_identical(
    geoparquet_bbox(
      geoarrow_create_narrow_from_buffers(
        wk::wkt("LINESTRING M (0 1 2, 2 3 4)")
      )
    ),
    c(0, 1, 2, 3)
  )

  expect_identical(
    geoparquet_bbox(
      geoarrow_create_narrow_from_buffers(
        wk::wkt("LINESTRING ZM (0 1 2 3, 2 3 4 5)")
      )
    ),
    c(0, 1, 2, 3)
  )
})

test_that("schema_from_geoparquet_metadata() works for WKT", {
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = NULL, encoding = "WKT"),
    narrow::narrow_schema(name = "", format = "u")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt()
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = "EPSG:1234", encoding = "WKT"),
    narrow::narrow_schema(format = "u", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(crs = "EPSG:1234")
    )
  )

  # with default assumed CRS
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(encoding = "WKT"),
    narrow::narrow_schema(format = "u", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(crs = geoparquet_crs_if_missing())
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = NULL, edges = "spherical", encoding = "WKT"),
    narrow::narrow_schema(format = "u", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(edges = "spherical")
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = NULL, encoding = "WKT"),
    narrow::narrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(format = "w:12")
    )
  )
})

test_that("schema_from_geoparquet_metadata() works for WKB", {
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = NULL, encoding = "WKB"),
    narrow::narrow_schema(name = "", format = "z")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb()
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = "EPSG:1234", encoding = "WKB"),
    narrow::narrow_schema(format = "z", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb(crs = "EPSG:1234")
    )
  )

  # with default assumed CRS
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(encoding = "WKT"),
    narrow::narrow_schema(format = "u", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(crs = geoparquet_crs_if_missing())
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = NULL, edges = "spherical", encoding = "WKB"),
    narrow::narrow_schema(format = "z", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb(edges = "spherical")
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = NULL, encoding = "WKB"),
    narrow::narrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb(format = "w:12")
    )
  )
})

test_that("schema_from_geoparquet_metadata() works for point", {
  bare_point <- geoarrow_schema_point(dim = "xy")
  bare_point$metadata <- NULL

  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = NULL, encoding = "geoarrow.point"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = "EPSG:1234", encoding = "geoarrow.point"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point(crs = "EPSG:1234"),
      recursive = TRUE
    )
  )

  # with dim
  bare_point$format <- "+w:4"
  bare_point$children[[1]]$name <- ""
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(crs = "EPSG:1234", encoding = "geoarrow.point"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point(dim = "xyzm", crs = "EPSG:1234"),
      recursive = TRUE
    )
  )
})

test_that("schema_from_geoparquet_metadata() works for linestring", {
  bare <- geoarrow_schema_linestring()
  bare$metadata <- NULL

  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      ),
      recursive = TRUE
    )
  )

  # with dim
  bare$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL, edges = "spherical",
      encoding = "geoarrow.linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(
        edges = "spherical",
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )
})

test_that("schema_from_geoparquet_metadata() works for polygon", {
  bare <- geoarrow_schema_polygon()
  bare$metadata <- NULL

  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      ),
      recursive = TRUE
    )
  )

  # with dim
  bare$children[[1]]$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL, edges = "spherical",
      encoding = "geoarrow.polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(
        edges = "spherical",
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )
})

test_that("schema_from_geoparquet_metadata() works for multipoint", {
  bare <- geoarrow_schema_multipoint()
  bare$metadata <- NULL
  bare$children[[1]]$metadata <- NULL
  bare$children[[1]]$children[[1]]$metadata <- NULL

  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.multipoint"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multipoint(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.multipoint"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multipoint(crs = "EPSG:1234"),
      recursive = TRUE
    )
  )

  # with dim
  bare$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.multipoint"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multipoint(dim = "xyzm"),
      recursive = TRUE
    )
  )
})

test_that("schema_from_geoparquet_metadata() works for multilinestring", {
  bare <- geoarrow_schema_multilinestring()
  bare$metadata <- NULL
  bare$children[[1]]$metadata <- NULL
  bare$children[[1]]$children[[1]]$metadata <- NULL

  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multilinestring(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multilinestring(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      ),
      recursive = TRUE
    )
  )

  # with dim
  bare$children[[1]]$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multilinestring(
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL, edges = "spherical",
      encoding = "geoarrow.multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multilinestring(
        edges = "spherical",
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )
})

test_that("schema_from_geoparquet_metadata() works for multipolygon", {
  bare <- geoarrow_schema_multipolygon()
  bare$metadata <- NULL
  bare$children[[1]]$metadata <- NULL
  bare$children[[1]]$children[[1]]$metadata <- NULL

  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multipolygon(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = "EPSG:1234",
      encoding = "geoarrow.multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multipolygon(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      ),
      recursive = TRUE
    )
  )

  # with dim
  bare$children[[1]]$children[[1]]$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$children[[1]]$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL,
      encoding = "geoarrow.multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multipolygon(
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_geoparquet_metadata(
    list(
      crs = NULL, edges = "spherical",
      encoding = "geoarrow.multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multipolygon(
        edges = "spherical",
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )
})

test_that("guess_column_encoding() works for extensioned arrays", {
  expect_identical(
    guess_column_encoding(geoarrow_schema_point()),
    "geoarrow.point"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_linestring()),
    "geoarrow.linestring"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_polygon()),
    "geoarrow.polygon"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_multipoint()),
    "geoarrow.multipoint"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_multilinestring()),
    "geoarrow.multilinestring"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_multipolygon()),
    "geoarrow.multipolygon"
  )

  schema <- geoarrow_schema_multipoint()
  schema$children[[1]] <- geoarrow_schema_multipoint()
  expect_error(guess_column_encoding(schema), "Unsupported child encoding for collection")
})

test_that("guess_column_encoding() works for unextensioned arrays", {
  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("w:10")
    ),
    "WKB"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("z")
    ),
    "WKB"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("Z")
    ),
    "WKB"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("u")
    ),
    "WKT"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("U")
    ),
    "WKT"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("+w:2", children = list(narrow::narrow_schema("f")))
    ),
    "geoarrow.point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("+w:4", children = list(narrow::narrow_schema("g")))
    ),
    "geoarrow.point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:02",
        children = list(narrow::narrow_schema("g", name = "xy"))
      )
    ),
    "geoarrow.point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:03",
        children = list(narrow::narrow_schema("g", name = "xyz"))
      )
    ),
    "geoarrow.point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:03",
        children = list(narrow::narrow_schema("g", name = "xym"))
      )
    ),
    "geoarrow.point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:04",
        children = list(narrow::narrow_schema("g", name = "xyzm"))
      )
    ),
    "geoarrow.point"
  )

  schema <- geoarrow_schema_point(dim = "xy")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.point")

  schema <- geoarrow_schema_point(dim = "xyz")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.point")

  schema <- geoarrow_schema_point(dim = "xym")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.point")

  schema <- geoarrow_schema_point(dim = "xyzm")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.point")

  schema <- geoarrow_schema_linestring()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.linestring")

  schema <- geoarrow_schema_polygon()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.polygon")

  schema <- geoarrow_schema_multipoint()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.multipoint")

  schema <- geoarrow_schema_multilinestring()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.multilinestring")

  schema <- geoarrow_schema_multipolygon()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "geoarrow.multipolygon")

  schema <- geoarrow_schema_multipoint()
  schema$metadata <- NULL
  schema$children[[1]]$name <- "geometries"
  expect_identical(guess_column_encoding(schema), "geoarrow.multipoint")

  expect_error(
    guess_column_encoding(narrow::narrow_schema("i")),
    "Can't guess encoding for schema"
  )

  expect_error(
    guess_column_encoding(
      narrow::narrow_schema(
        "+l",
        children = list(narrow::narrow_schema("i", name = "not_recognized"))
      )
    ),
    "Can't guess encoding for schema"
  )
})

test_that("guess_column_dim() works", {
  expect_identical(
    guess_column_dim(geoarrow_schema_point(dim = "xyz")),
    "xyz"
  )

  bare_schema <- geoarrow_schema_point(dim = "xy")
  bare_schema$children[[1]]$name <- ""
  expect_identical(
    guess_column_dim(bare_schema),
    "xy"
  )

  bare_schema <- geoarrow_schema_point(dim = "xyzm")
  bare_schema$children[[1]]$name <- ""
  expect_identical(
    guess_column_dim(bare_schema),
    "xyzm"
  )

  expect_identical(
    guess_column_dim(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(dim = "xyz")
      )
    ),
    "xyz"
  )

  expect_identical(guess_column_dim(narrow::narrow_schema("i")), NULL)
})

