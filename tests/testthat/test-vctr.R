
test_that("geoarrow_vctr class works", {
  vctr <- geoarrow(wk::wkt(c("POINT (1 2)", NA)))
  expect_s3_class(vctr, "geoarrow_point")
  expect_length(vctr, 2)
  expect_s3_class(attr(vctr, "schema"), "narrow_schema")
  expect_s3_class(attr(vctr, "array_data")[[1]], "narrow_array_data")
  expect_identical(vctrs::vec_proxy(vctr), vctr)
  expect_s3_class(vctrs::vec_restore(vctr, vctr), "geoarrow_point")
})

test_that("geoarrow_vctrs can be in data.frames", {
  df <- data.frame(vctr = geoarrow(wk::wkt(c("POINT (1 2)", NA))))
  expect_identical(wk::as_xy(df$vctr), wk::xy(c(1, NA), c(2, NA)))
})

test_that("geoarrow_vctrs can be in tibbles", {
  df <- tibble::tibble(vctr = geoarrow(wk::wkt(c("POINT (1 2)", NA))))
  expect_identical(wk::as_xy(df$vctr), wk::xy(c(1, NA), c(2, NA)))
})

test_that("format, str, as.character, and print methods work for geoarrow_vctr", {
  vctr <- geoarrow(wk::wkt(c("POINT (1 2)", NA)))

  expect_identical(
    format(vctr),
    c("POINT (1 2)", "<null feature>")
  )

  expect_identical(
    as.character(vctr),
    c("POINT (1 2)", NA)
  )

  expect_output(print(vctr[integer()]), "geoarrow_point\\[0\\]")
  expect_output(expect_identical(print(vctr), vctr), "geoarrow_point\\[2\\]")
  expect_output(str(vctr[integer()]), "geoarrow_point\\[0\\]")
  expect_output(expect_identical(str(vctr), vctr), "geoarrow_point\\[1:2\\]")
})

test_that("subset methods work for geoarrow_vctr", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))

  expect_identical(wk::as_xy(vctr[]), wk::xy(1:4, 5:8))
  expect_identical(wk::as_xy(vctr[1]), wk::xy(1, 5))
  expect_identical(wk::as_xy(vctr[integer()]), wk::xy(crs = NULL))

  expect_identical(
    wk::as_xy(vctr[[1]]),
    wk::as_xy(vctr[1])
  )
})

test_that("rep method works for geoarrow_vctr", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))

  expect_identical(rep(vctr, 0), vctr[integer()])
  expect_identical(rep(vctr, 1), vctr)
  expect_identical(rep(vctr, 2), vctr[c(1:4, 1:4)])
  expect_identical(rep(vctr, each = 2), vctr[c(1, 1, 2, 2, 3, 3, 4, 4)])

  expect_identical(wk::as_xy(rep(vctr, 0)), wk::xy(crs = NULL))
  expect_identical(wk::as_xy(rep(vctr, 1)), wk::xy(1:4, 5:8))
  expect_identical(wk::as_xy(rep(vctr, 2)), wk::xy(c(1:4, 1:4), c(5:8, 5:8)))
  expect_identical(
    wk::as_xy(rep(vctr, each = 2)),
    wk::xy(c(1, 1, 2, 2, 3, 3, 4, 4), c(5, 5, 6, 6, 7, 7, 8, 8))
  )

  expect_identical(wk::as_xy(vctr[1]), wk::xy(1, 5))
  expect_identical(wk::as_xy(vctr[integer()]), wk::xy(crs = NULL))

  expect_identical(
    wk::as_xy(vctr[[1]]),
    wk::as_xy(vctr[1])
  )
})

test_that("rep_len method works for geoarrow_vctr", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))

  expect_identical(rep_len(vctr, 0), vctr[integer()])
  expect_identical(rep_len(vctr, 1), vctr[1])
  expect_identical(rep_len(vctr, 4), vctr)
  expect_identical(rep_len(vctr, 8), rep(vctr, 2))
})

test_that("as_narrow_array() works for geoarrow_vctr", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  array_data <- attr(vctr, "array_data")[[1]]

  # freshly constructed version shouldn't reallocate
  expect_identical(as_narrow_array(vctr)$array_data, array_data)

  # subsetted version
  vctr_subset <- vctr_restore(2L, vctr)
  expect_identical(
    wk::wk_handle(narrow::as_narrow_array(vctr_subset), wk::xy_writer()),
    wk::xy(2, 6)
  )

  # concatenated version
  attr(vctr, "array_data") <- c(list(array_data, array_data))
  vctr_concat <- vctr_restore(c(4L, 5L), vctr)
  expect_identical(
    wk::wk_handle(narrow::as_narrow_array(vctr_concat), wk::xy_writer()),
    wk::xy(c(4, 1), c(8, 5))
  )
})

test_that("vctrs support works for all extensions", {
  vctr <- geoarrow_wkt(wk::wkt(c("POINT (1 2)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_wkt"
  )

  vctr <- geoarrow_wkb(wk::wkt(c("POINT (1 2)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_wkb"
  )

  vctr <- geoarrow(wk::wkt(c("POINT (1 2)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_point"
  )

  vctr <- geoarrow(wk::wkt(c("LINESTRING (1 2, 3 4)")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_linestring"
  )

  vctr <- geoarrow(wk::wkt(c("POLYGON ((1 2, 3 4, 5 6, 1 2))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_polygon"
  )

  vctr <- geoarrow(wk::wkt(c("MULTIPOINT ((1 2), (3 4))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_multipoint"
  )

  vctr <- geoarrow(wk::wkt(c("MULTILINESTRING ((1 2, 3 4))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_multilinestring"
  )

  vctr <- geoarrow(wk::wkt(c("MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))")))
  expect_true(vctrs::vec_is(vctr))
  expect_s3_class(
    vctrs::vec_restore(vctrs::vec_proxy(vctr), vctr),
    "geoarrow_multipolygon"
  )
})

test_that("identity/slice/is increasing detector works", {
  expect_false(is_identity_slice(environment(), 0))

  expect_false(is_slice(integer()))
  expect_true(is_slice(1L))
  expect_true(is_slice(1:3))
  expect_true(is_slice(2:4))

  expect_false(is_identity_slice(integer(), 0))
  expect_true(is_identity_slice(1L, 1))
  expect_true(is_identity_slice(1:3, 3))
  expect_false(is_identity_slice(2:4, 3))

  expect_true(all_increasing(integer()))
  expect_true(all_increasing(1))
  expect_true(all_increasing(1:5))
  expect_true(all_increasing(c(1, 2, 2, 3)))
  expect_true(all_increasing(c(1, NA, 2, 2, NA, 2, 3)))
  expect_false(all_increasing(c(2, 1)))
})
