
test_that("geoarrow vector class works", {
  skip_if_not(has_arrow_with_extension_type())

  vctr <- geoarrow_create(wk::wkt(c("POINT (1 2)", NA)))
  expect_s3_class(vctr, "narrow_vctr_geoarrow_point")
  expect_length(vctr, 2)
  expect_identical(vctrs::vec_proxy(vctr), vctr)
  expect_s3_class(vctrs::vec_restore(vctr, vctr), "narrow_vctr_geoarrow_point")
})

test_that("geoarrow format() works for all extensions", {
  skip_if_not(has_arrow_with_extension_type())

  expect_identical(
    format(geoarrow_create(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkt())),
    "POINT (1 2)"
  )

  expect_identical(
    format(geoarrow_create(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkb())),
    "POINT (1 2)"
  )

  expect_identical(
    format(geoarrow_create(wk::wkt(c("POINT (1 2)")))),
    "POINT (1 2)"
  )

  expect_identical(
    format(geoarrow_create(wk::wkt(c("LINESTRING (1 2, 3 4)")))),
    "LINESTRING (1 2, 3 4)"
  )

  expect_identical(
    format(geoarrow_create(wk::wkt(c("POLYGON ((1 2, 3 4, 5 6, 1 2))")))),
    "POLYGON ((1 2, 3 4, 5 6, 1 2))"
  )

  expect_identical(
    format(geoarrow_create(wk::wkt(c("MULTIPOINT ((1 2), (3 4))")))),
    "MULTIPOINT ((1 2), (3 4))"
  )
})

test_that("geoarrow as.character() works for all extensions", {
  skip_if_not(has_arrow_with_extension_type())

  expect_identical(
    as.character(geoarrow_create(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkt())),
    "POINT (1 2)"
  )

  expect_identical(
    as.character(geoarrow_create(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkb())),
    "POINT (1 2)"
  )

  expect_identical(
    as.character(geoarrow_create(wk::wkt(c("POINT (1 2)")))),
    "POINT (1 2)"
  )

  expect_identical(
    as.character(geoarrow_create(wk::wkt(c("LINESTRING (1 2, 3 4)")))),
    "LINESTRING (1 2, 3 4)"
  )

  expect_identical(
    as.character(geoarrow_create(wk::wkt(c("POLYGON ((1 2, 3 4, 5 6, 1 2))")))),
    "POLYGON ((1 2, 3 4, 5 6, 1 2))"
  )

  expect_identical(
    as.character(geoarrow_create(wk::wkt(c("MULTIPOINT ((1 2), (3 4))")))),
    "MULTIPOINT ((1 2), (3 4))"
  )
})

test_that("vctrs support works for all extensions", {
  skip_if_not(has_arrow_with_extension_type())

  vctr <- geoarrow_create(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkt())
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_wkt"
  )

  vctr <- geoarrow_create(wk::wkt(c("POINT (1 2)")), schema = geoarrow_schema_wkb())
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_wkb"
  )

  vctr <- geoarrow_create(wk::wkt(c("POINT (1 2)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_point"
  )

  vctr <- geoarrow_create(wk::wkt(c("LINESTRING (1 2, 3 4)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_linestring"
  )

  vctr <- geoarrow_create(wk::wkt(c("POLYGON ((1 2, 3 4, 5 6, 1 2))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_polygon"
  )

  vctr <- geoarrow_create(wk::wkt(c("MULTIPOINT ((1 2), (3 4))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_multipoint"
  )

  vctr <- geoarrow_create(wk::wkt(c("MULTILINESTRING ((1 2, 3 4))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_multilinestring"
  )

  vctr <- geoarrow_create(wk::wkt(c("MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "narrow_vctr_geoarrow_multipolygon"
  )
})
