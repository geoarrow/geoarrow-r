
test_that("WKT reader works with multipoint", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT EMPTY")),
    wk::new_wk_wkt("MULTIPOINT EMPTY")
  )

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT (EMPTY)")),
    wk::new_wk_wkt("MULTIPOINT (EMPTY)")
  )

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT (10 40)")),
    wk::new_wk_wkt("MULTIPOINT ((10 40))")
  )

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT ((10 40))")),
    wk::new_wk_wkt("MULTIPOINT ((10 40))")
  )

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT (10 40)")),
    wk::new_wk_wkt("MULTIPOINT ((10 40))")
  )

  wkt <- "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt(wkt)),
    wk::new_wk_wkt(wkt)
  )

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTIPOINT (10 40, 40 30, 20 20, 30 10)")),
    wk::new_wk_wkt(wkt)
  )
})

test_that("WKT reader works with multilinestring", {
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING EMPTY")),
    wk::new_wk_wkt("MULTILINESTRING EMPTY")
  )

  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING (EMPTY)")),
    wk::new_wk_wkt("MULTILINESTRING (EMPTY)")
  )

  wkt <- "MULTILINESTRING ((10 40, 40 30, 20 20, 30 10))"
  expect_identical(
    wk::as_wkt(geoarrow_create_wkt("MULTILINESTRING ((10 40, 40 30, 20 20, 30 10))")),
    wk::new_wk_wkt(wkt)
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
  expect_error(
    wk::wk_void(geoarrow_create_wkt(strrep("a", 4096))),
    "Expected a value with fewer than 4096 character"
  )
})
