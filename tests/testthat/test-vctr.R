
test_that("geoarrow vector class works", {
  vctr <- as_geoarrow_vctr(wk::wkt(c("POINT (1 2)", NA)))
  expect_s3_class(vctr, "narrow_vctr_geoarrow_point")
  expect_length(vctr, 2)
  expect_identical(vctrs::vec_proxy(vctr), vctr)
  expect_s3_class(vctrs::vec_restore(vctr, vctr), "narrow_vctr_geoarrow_point")
})

test_that("geoarrow format() works for all extensions", {
  expect_identical(
    format(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkt())),
    "POINT (1 2)"
  )

  expect_identical(
    format(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkb())),
    "POINT (1 2)"
  )

  expect_identical(
    format(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")))),
    "POINT (1 2)"
  )

  expect_identical(
    format(as_geoarrow_vctr(wk::wkt(c("LINESTRING (1 2, 3 4)")))),
    "LINESTRING (1 2, 3 4)"
  )

  expect_identical(
    format(as_geoarrow_vctr(wk::wkt(c("POLYGON ((1 2, 3 4, 5 6, 1 2))")))),
    "POLYGON ((1 2, 3 4, 5 6, 1 2))"
  )

  expect_identical(
    format(as_geoarrow_vctr(wk::wkt(c("MULTIPOINT ((1 2), (3 4))")))),
    "MULTIPOINT ((1 2), (3 4))"
  )
})

test_that("geoarrow as.character() works for all extensions", {
  expect_identical(
    as.character(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkt())),
    "POINT (1 2)"
  )

  expect_identical(
    as.character(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkb())),
    "POINT (1 2)"
  )

  expect_identical(
    as.character(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")))),
    "POINT (1 2)"
  )

  expect_identical(
    as.character(as_geoarrow_vctr(wk::wkt(c("LINESTRING (1 2, 3 4)")))),
    "LINESTRING (1 2, 3 4)"
  )

  expect_identical(
    as.character(as_geoarrow_vctr(wk::wkt(c("POLYGON ((1 2, 3 4, 5 6, 1 2))")))),
    "POLYGON ((1 2, 3 4, 5 6, 1 2))"
  )

  expect_identical(
    as.character(as_geoarrow_vctr(wk::wkt(c("MULTIPOINT ((1 2), (3 4))")))),
    "MULTIPOINT ((1 2), (3 4))"
  )
})

test_that("vctrs support works for all extensions", {
  vctr <- as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkt())
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_wkt"
  )

  vctr <- as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkb())
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_wkb"
  )

  vctr <- as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_point"
  )

  vctr <- as_geoarrow_vctr(wk::wkt(c("LINESTRING (1 2, 3 4)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_linestring"
  )

  vctr <- as_geoarrow_vctr(wk::wkt(c("POLYGON ((1 2, 3 4, 5 6, 1 2))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_polygon"
  )

  vctr <- as_geoarrow_vctr(wk::wkt(c("MULTIPOINT ((1 2), (3 4))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_multi"
  )
})

test_that("format and as.character() work for geojson extension", {
  skip_if_not_installed("geos")

  expect_identical(
    format(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_geojson())),
    "POINT (1 2)"
  )

  expect_identical(
    as.character(as_geoarrow_vctr(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_geojson())),
    "POINT (1 2)"
  )
})
