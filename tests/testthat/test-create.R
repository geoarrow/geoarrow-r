
test_that("geoarrow_create_narrow() can use geoarrow_compute() for WKT", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::xy(1:2, 1:2)),
    schema = geoarrow_schema_wkt()
  )

  expect_identical(
    as.character(narrow::from_narrow_array(array)),
    c("POINT (1 1)", "POINT (2 2)")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for WKB", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::xy(1:2, 1:2)),
    schema = geoarrow_schema_wkb()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})


test_that("geoarrow_create_narrow() can use geoarrow_compute() for Large WKB", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::xy(1:2, 1:2)),
    schema = geoarrow_schema_wkb(format = "Z"),
    strict = TRUE
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for Fixed-size WKB", {
  skip_if_not(has_arrow_with_extension_type())

  array_arrow <- arrow::Array$create(
    unclass(wk::as_wkb(wk::xy(1:2, 1:2))),
    arrow::fixed_size_binary(byte_width = 21)
  )

  array <- narrow::as_narrow_array(array_arrow)
  array$schema$metadata[["ARROW:extension:name"]] <- "geoarrow.wkb"

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for point", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::xy(1:2, 1:2)),
    schema = geoarrow_schema_point()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt(c("POINT (1 1)", "POINT (2 2)"))
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for linestring", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::wkt("LINESTRING (0 1, 2 3)")),
    schema = geoarrow_schema_linestring()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("LINESTRING (0 1, 2 3)")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for polygon", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))")),
    schema = geoarrow_schema_polygon()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("POLYGON ((0 0, 1 0, 0 1, 0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for multipoint", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::wkt("MULTIPOINT ((0 0), (1 0), (0 1), (0 0))")),
    schema = geoarrow_schema_multipoint()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTIPOINT ((0 0), (1 0), (0 1), (0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for multilinestring", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::wkt("MULTILINESTRING ((0 0, 1 0, 0 1, 0 0))")),
    schema = geoarrow_schema_multilinestring()
  )

  expect_identical(
    wk::as_wkt(array),
    wk::wkt("MULTILINESTRING ((0 0, 1 0, 0 1, 0 0))")
  )
})

test_that("geoarrow_create_narrow() can use geoarrow_compute() for multipolygon", {
  array <- geoarrow_create_narrow(
    geoarrow_create_narrow(wk::wkt("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))")),
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

test_that("geoarrow_schema_default() promotes to multi when simple + multi are included", {
  schema_multipoint <- geoarrow_schema_default(
    wk::wkt(c("POINT (0 1)", "MULTIPOINT (1 1, 2 2)"))
  )
  expect_identical(
    schema_multipoint$metadata[["ARROW:extension:name"]],
    "geoarrow.multipoint"
  )

  schema_multilinestring <- geoarrow_schema_default(
    wk::wkt(c("LINESTRING (0 0, 1 1 )", "MULTILINESTRING ((1 1, 2 2))"))
  )
  expect_identical(
    schema_multilinestring$metadata[["ARROW:extension:name"]],
    "geoarrow.multilinestring"
  )

  schema_multipolygon <- geoarrow_schema_default(
    wk::wkt(c("POLYGON ((0 0, 0 1, 1 0, 0 0))", "MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)))"))
  )
  expect_identical(
    schema_multipolygon$metadata[["ARROW:extension:name"]],
    "geoarrow.multipolygon"
  )
})

test_that("geoarrow_schema_default() ignores empties", {
  schema_multipoint <- geoarrow_schema_default(
    wk::wkt(c("GEOMETRYCOLLECTION EMPTY", "MULTIPOINT (1 1, 2 2)"))
  )
  expect_identical(
    schema_multipoint$metadata[["ARROW:extension:name"]],
    "geoarrow.multipoint"
  )

  schema_multilinestring <- geoarrow_schema_default(
    wk::wkt(c("GEOMETRYCOLLECTION EMPTY", "MULTILINESTRING ((1 1, 2 2))"))
  )
  expect_identical(
    schema_multilinestring$metadata[["ARROW:extension:name"]],
    "geoarrow.multilinestring"
  )

  schema_multipolygon <- geoarrow_schema_default(
    wk::wkt(c("GEOMETRYCOLLECTION EMPTY", "MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)))"))
  )
  expect_identical(
    schema_multipolygon$metadata[["ARROW:extension:name"]],
    "geoarrow.multipolygon"
  )
})

test_that("geoarrow_schema_default() falls back to WKB for mixed type vectors", {
  schema_collection <- geoarrow_schema_default(wk::wkt("GEOMETRYCOLLECTION (POINT (1 2))"))
  expect_identical(
    schema_collection$metadata[["ARROW:extension:name"]],
    "geoarrow.wkb"
  )

  schema_mixed <- geoarrow_schema_default(wk::wkt(c("LINESTRING (1 2)", "MULTIPOINT (1 2, 3 4)")))
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

test_that("geoarrow_schema_default() detects dimensions without vector_meta", {
  schema_point <- geoarrow_schema_default(wk::wkt("POINT (1 1)"))
  expect_identical(schema_point$children[[1]]$name, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::wkt("POINT Z (1 1 1)"))
  expect_identical(schema_point_xyz$children[[1]]$name, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::wkt("POINT M (1 1 1)"))
  expect_identical(schema_point_xym$children[[1]]$name, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::wkt("POINT ZM (1 1 1 1)"))
  expect_identical(schema_point_xyzm$children[[1]]$name, "xyzm")
})

test_that("geoarrow_schema_default() detects and sets CRS", {
  schema_point <- geoarrow_schema_default(
    wk::wkt("POINT (1 1)", crs = "EPSG:1234")
  )
  expect_identical(geoarrow_metadata(schema_point)$crs, "EPSG:1234")

  schema_linestring <- geoarrow_schema_default(
    wk::wkt("LINESTRING (1 1, 2 2)", crs = "EPSG:1234")
  )
  expect_identical(geoarrow_metadata(schema_linestring$children[[1]])$crs, "EPSG:1234")

  schema_polygon <- geoarrow_schema_default(
    wk::wkt("POLYGON ((0 0, 0 1, 1 0, 0 0))", crs = "EPSG:1234")
  )
  expect_identical(
    geoarrow_metadata(schema_polygon$children[[1]]$children[[1]])$crs,
    "EPSG:1234"
  )

  schema_multipoint <- geoarrow_schema_default(
    wk::wkt("MULTIPOINT ((1 1))", crs = "EPSG:1234")
  )
  expect_identical(
    geoarrow_metadata(schema_multipoint$children[[1]])$crs,
    "EPSG:1234"
  )

  schema_multilinestring <- geoarrow_schema_default(
    wk::wkt("MULTILINESTRING ((1 1, 2 2))", crs = "EPSG:1234")
  )
  expect_identical(
    geoarrow_metadata(schema_multilinestring$children[[1]]$children[[1]])$crs,
    "EPSG:1234"
  )

  schema_multipolygon <- geoarrow_schema_default(
    wk::wkt("MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)))", crs = "EPSG:1234")
  )
  expect_identical(
    geoarrow_metadata(schema_multipolygon$children[[1]]$children[[1]]$children[[1]])$crs,
    "EPSG:1234"
  )

  schema_mixed <- geoarrow_schema_default(
    wk::wkt(c("POINT (0 1)", "LINESTRING (0 0, 1 1)"), crs = "EPSG:1234")
  )
  expect_identical(
    geoarrow_metadata(schema_mixed)$crs,
    "EPSG:1234"
  )
})

test_that("geoarrow_schema_default() detects and sets edges", {
  schema_linestring <- geoarrow_schema_default(
    wk::wkt("LINESTRING (1 1, 2 2)", geodesic = TRUE)
  )
  expect_identical(geoarrow_metadata(schema_linestring)$edges, "spherical")

  schema_polygon <- geoarrow_schema_default(
    wk::wkt("POLYGON ((0 0, 0 1, 1 0, 0 0))", geodesic = TRUE)
  )
  expect_identical(
    geoarrow_metadata(schema_polygon)$edges,
    "spherical"
  )

  schema_multilinestring <- geoarrow_schema_default(
    wk::wkt("MULTILINESTRING ((1 1, 2 2))", geodesic = TRUE)
  )
  expect_identical(
    geoarrow_metadata(schema_multilinestring$children[[1]])$edges,
    "spherical"
  )

  schema_multipolygon <- geoarrow_schema_default(
    wk::wkt("MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)))", geodesic = TRUE)
  )
  expect_identical(
    geoarrow_metadata(schema_multipolygon$children[[1]])$edges,
    "spherical"
  )

  schema_mixed <- geoarrow_schema_default(
    wk::wkt(c("POINT (0 1)", "LINESTRING (0 0, 1 1)"), geodesic = TRUE)
  )
  expect_identical(
    geoarrow_metadata(schema_mixed)$edges,
    "spherical"
  )
})
