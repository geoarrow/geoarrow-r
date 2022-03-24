
test_that("WKT reader works on non-empty 2D geoms", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POINT (30 10)")),
    wk::new_wk_wkt("POINT (30 10)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("LINESTRING (30 10, 0 0)")),
    wk::new_wk_wkt("LINESTRING (30 10, 0 0)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POLYGON ((30 10, 0 0, 10 10, 30 10))")),
    wk::new_wk_wkt("POLYGON ((30 10, 0 0, 10 10, 30 10))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")),
    wk::new_wk_wkt("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT (30 10, 0 0, 10 10)")),
    wk::new_wk_wkt("MULTIPOINT ((30 10), (0 0), (10 10))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT ((30 10), (0 0), (10 10))")),
    wk::new_wk_wkt("MULTIPOINT ((30 10), (0 0), (10 10))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING ((30 10, 0 0), (20 20, 0 0))")),
    wk::new_wk_wkt("MULTILINESTRING ((30 10, 0 0), (20 20, 0 0))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOLYGON (((30 10, 0 0, 10 10, 30 10)), ((30 10, 0 0, 10 10, 30 10)))")),
    wk::new_wk_wkt("MULTIPOLYGON (((30 10, 0 0, 10 10, 30 10)), ((30 10, 0 0, 10 10, 30 10)))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt(
      "GEOMETRYCOLLECTION (POINT (30 10), GEOMETRYCOLLECTION (POINT (12 6)), LINESTRING (1 2, 3 4))"
    )),
    wk::new_wk_wkt("GEOMETRYCOLLECTION (POINT (30 10), GEOMETRYCOLLECTION (POINT (12 6)), LINESTRING (1 2, 3 4))")
  )
})

test_that("WKT reader works on empty geoms", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POINT EMPTY")),
    wk::new_wk_wkt("POINT EMPTY")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("LINESTRING EMPTY")),
    wk::new_wk_wkt("LINESTRING EMPTY")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POLYGON EMPTY")),
    wk::new_wk_wkt("POLYGON EMPTY")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT EMPTY")),
    wk::new_wk_wkt("MULTIPOINT EMPTY")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING EMPTY")),
    wk::new_wk_wkt("MULTILINESTRING EMPTY")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOLYGON EMPTY")),
    wk::new_wk_wkt("MULTIPOLYGON EMPTY")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt(
      "GEOMETRYCOLLECTION EMPTY"
    )),
    wk::new_wk_wkt("GEOMETRYCOLLECTION EMPTY")
  )
})

test_that("WKT reader works with NULLs", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt(NA_character_)),
    wk::new_wk_wkt(NA_character_)
  )
})

test_that("WKT reader mutli* geometries can contain empties", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT (EMPTY)")),
    wk::new_wk_wkt("MULTIPOINT (EMPTY)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT (1 1, EMPTY)")),
    wk::new_wk_wkt("MULTIPOINT ((1 1), EMPTY)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT ((1 1), EMPTY)")),
    wk::new_wk_wkt("MULTIPOINT ((1 1), EMPTY)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING (EMPTY)")),
    wk::new_wk_wkt("MULTILINESTRING (EMPTY)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING ((1 1, 2 4), EMPTY)")),
    wk::new_wk_wkt("MULTILINESTRING ((1 1, 2 4), EMPTY)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOLYGON (((1 1, 2 4, 3 6)), EMPTY)")),
    wk::new_wk_wkt("MULTIPOLYGON (((1 1, 2 4, 3 6)), EMPTY)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOLYGON (EMPTY)")),
    wk::new_wk_wkt("MULTIPOLYGON (EMPTY)")
  )
})

test_that("WKT reader Z, ZM, and M prefixes are parsed", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POINT (30 10)")),
    wk::new_wk_wkt("POINT (30 10)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POINT Z (30 10 1)")),
    wk::new_wk_wkt("POINT Z (30 10 1)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POINT M (30 10 1)")),
    wk::new_wk_wkt("POINT M (30 10 1)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POINT ZM (30 10 0 1)")),
    wk::new_wk_wkt("POINT ZM (30 10 0 1)")
  )
})

test_that("WKT reader SRID prefixes are parsed (but dropped)", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("SRID=218;POINT (30 10)")),
    wk::new_wk_wkt("POINT (30 10)")
  )
})

test_that("WKT reader parses correctly formatted ZM geomteries", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POINT ZM (30 10 0 1)")),
    wk::new_wk_wkt("POINT ZM (30 10 0 1)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("LINESTRING ZM (30 10 0 1, 0 0 2 3)")),
    wk::new_wk_wkt("LINESTRING ZM (30 10 0 1, 0 0 2 3)")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("POLYGON ZM ((30 10 2 1, 0 0 9 10, 10 10 10 8, 30 10 3 8))")),
    wk::new_wk_wkt("POLYGON ZM ((30 10 2 1, 0 0 9 10, 10 10 10 8, 30 10 3 8))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT ZM (30 10 32 1, 0 0 2 8, 10 10 1 99)")),
    wk::new_wk_wkt("MULTIPOINT ZM ((30 10 32 1), (0 0 2 8), (10 10 1 99))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT ZM ((30 10 32 1), (0 0 2 8), (10 10 1 99))")),
    wk::new_wk_wkt("MULTIPOINT ZM ((30 10 32 1), (0 0 2 8), (10 10 1 99))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING ZM ((30 10 2 1, 0 0 2 8), (20 20 1 1, 0 0 2 2))")),
    wk::new_wk_wkt("MULTILINESTRING ZM ((30 10 2 1, 0 0 2 8), (20 20 1 1, 0 0 2 2))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOLYGON ZM (((30 10 1 3, 0 0 9 1, 10 10 5 9, 30 10 1 2)))")),
    wk::new_wk_wkt("MULTIPOLYGON ZM (((30 10 1 3, 0 0 9 1, 10 10 5 9, 30 10 1 2)))")
  )
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("GEOMETRYCOLLECTION (POINT ZM (30 10 1 2))")),
    wk::new_wk_wkt("GEOMETRYCOLLECTION (POINT ZM (30 10 1 2))")
  )
})

test_that("WKT reader works with nested collections", {
  wkt <-
    "GEOMETRYCOLLECTION (
    POINT (40 10),
    LINESTRING (10 10, 20 20, 10 40),
    POLYGON ((40 40, 20 45, 45 30, 40 40)),
    GEOMETRYCOLLECTION (
      POINT (40 10),
      LINESTRING (10 10, 20 20, 10 40),
      POLYGON ((40 40, 20 45, 45 30, 40 40))
    ),
    GEOMETRYCOLLECTION EMPTY,
    POINT (30 10)
  )"

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt(wkt)),
    wk::wk_handle(wk::new_wk_wkt(wkt), wk::wkt_writer())
  )
})

test_that("WKT reader can handle non-finite values", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT ((nan nan))")),
    wk::new_wk_wkt("MULTIPOINT ((nan nan))")
  )

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT ((inf -inf))")),
    wk::new_wk_wkt("MULTIPOINT ((inf -inf))")
  )
})

test_that("Invalid WKT fails with reasonable error messages", {
  # this tests all the ways (according to test coverage) that
  # the parser can throw an error
  expect_error(wk::wk_void(geoarrow_create_wkt("NOT WKT")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("MULTIPOINT (iambic 3)")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("SRID=fish;POINT (30 10)")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("SRID=")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("POINT (fish fish)")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("POINT (")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("POINT (3")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("POINT")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("POINT ")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("POINT (30 10=")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("POINT (30 10)P")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("LINESTRING (30 10, 0 0=")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("LINESTRING (30A")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("LINESTRING (30,")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("LINESTRING (30")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("SRID=30A")), "^Expected")
  expect_error(wk::wk_void(geoarrow_create_wkt("SRID")), "^Expected")
})


test_that("bad arrays error", {
  arr_wkt <- narrow::narrow_array(
    geoarrow_schema_wkt(format = "w:0"),
    narrow::narrow_array_data(buffers = list(raw(1), raw(1))),
    validate = FALSE
  )
  expect_error(wk::wk_void(arr_wkt), "to have width > 0")

  arr_wkt <- narrow::narrow_array(
    narrow::narrow_schema("i", metadata = list("ARROW:extension:name" = "geoarrow.wkt")),
    narrow::narrow_array_data(buffers = list(NULL, integer())),
    validate = FALSE
  )
  expect_error(
    wk::wk_void(arr_wkt),
    "Unsupported storage type for extension geoarrow.wkt"
  )
})

test_that("large binary/unicode is supported", {
  arr_wkt <- geoarrow_create_narrow_from_buffers(
    wk::wkt("POINT (0 1)"),
    schema = geoarrow_schema_wkt(format = "Z"),
    strict = TRUE
  )

  expect_identical(wk::as_wkt(arr_wkt), wk::wkt("POINT (0 1)"))

  arr_wkt <- geoarrow_create_narrow_from_buffers(
    wk::wkt("POINT (0 1)"),
    schema = geoarrow_schema_wkt(format = "U"),
    strict = TRUE
  )

  expect_identical(wk::as_wkt(arr_wkt), wk::wkt("POINT (0 1)"))
})

test_that("Early returns are supported from the WKT reader", {
  arr_wkt <- geoarrow_create_wkt("POINT Z (0 1 2)")
  expect_identical(wk::wk_vector_meta(arr_wkt)$size, 1)
  expect_identical(wk::wk_meta(arr_wkt)$geometry_type, 1L)
})
