
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

test_that("is.na() works for geoarrow_vctr", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  vctr_with_null_index <- vctr_restore(c(1L, NA_integer_, 2L), vctr)
  expect_identical(is.na(vctr_with_null_index), c(FALSE, TRUE, FALSE))

  vctr_with_null <- geoarrow(wk::wkt(c("POINT (0 1)", "POINT (1 2)", NA)))
  expect_identical(is.na(vctr_with_null), c(FALSE, FALSE, TRUE))

  vctr_with_both <- vctr_restore(c(1L, NA_integer_, 3L), vctr_with_null)
  expect_identical(is.na(vctr_with_both), c(FALSE, TRUE, TRUE))
})

test_that("rep_len method works for geoarrow_vctr", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))

  expect_identical(rep_len(vctr, 0), vctr[integer()])
  expect_identical(rep_len(vctr, 1), vctr[1])
  expect_identical(rep_len(vctr, 4), vctr)
  expect_identical(rep_len(vctr, 8), rep(vctr, 2))
})

test_that("as_narrow_array_stream() works for geoarrow_vctr with 0 or 1 array", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  schema <- attr(vctr, "schema")
  array_data <- attr(vctr, "array_data")[[1]]

  # freshly constructed version shouldn't reallocate
  stream <- narrow::as_narrow_array_stream(vctr)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )
  expect_identical(
    narrow::narrow_array_stream_get_next(stream)$array_data$buffers,
    array_data$buffers
  )
  expect_null(narrow::narrow_array_stream_get_next(stream))

  # empty indices version
  vctr_empty <- vctr_restore(integer(), vctr)
  stream <- narrow::as_narrow_array_stream(vctr_empty)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )
  expect_null(narrow::narrow_array_stream_get_next(stream))

  # empty arrays version
  vctr_empty <- vctr_restore(1L, vctr)
  attr(vctr_empty, "array_data") <- list()
  stream <- narrow::as_narrow_array_stream(vctr_empty)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )
  data <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(data$array_data$null_count, 1L)
  expect_identical(data$array_data$length, 1L)
  expect_null(narrow::narrow_array_stream_get_next(stream))
})

test_that("as_narrow_array_stream() works for geoarrow_vctr with >1 array", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  schema <- attr(vctr, "schema")
  array_data <- attr(vctr, "array_data")[[1]]

  attr(vctr, "array_data") <- list(array_data, array_data)
  vctr_concat <- vctr_restore(c(4L, 5L), vctr)

  stream <- narrow::as_narrow_array_stream(vctr_concat)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )

  array1 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array1), wk::xy(4, 8))
  array2 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array2), wk::xy(1, 5))

  expect_null(narrow::narrow_array_stream_get_next(stream))
})

test_that("as_narrow_array_stream() with geoarrow_vctr with >1 (empty) array", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  schema <- attr(vctr, "schema")
  array_data <- attr(vctr, "array_data")[[1]]
  array_data0 <- narrow::as_narrow_array(vctr[integer()])$array_data

  attr(vctr, "array_data") <- list(array_data, array_data0, array_data)
  vctr_concat <- vctr_restore(c(4L, 5L), vctr)

  stream <- narrow::as_narrow_array_stream(vctr_concat)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )

  array1 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array1), wk::xy(4, 8))
  array2 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array2), wk::xy(1, 5))

  expect_null(narrow::narrow_array_stream_get_next(stream))
})

test_that("as_narrow_array_stream() with geoarrow_vctr with >1 (leading NAs) array", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  schema <- attr(vctr, "schema")
  array_data <- attr(vctr, "array_data")[[1]]

  attr(vctr, "array_data") <- list(array_data, array_data)
  vctr_concat <- vctr_restore(c(NA_integer_, 4L, 5L), vctr)

  stream <- narrow::as_narrow_array_stream(vctr_concat)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )

  array1 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array1), wk::xy(c(NA, 4), c(NA, 8)))
  array2 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array2), wk::xy(1, 5))

  expect_null(narrow::narrow_array_stream_get_next(stream))
})

test_that("as_narrow_array_stream() with geoarrow_vctr with >1 (trailing NAs) array", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  schema <- attr(vctr, "schema")
  array_data <- attr(vctr, "array_data")[[1]]

  attr(vctr, "array_data") <- list(array_data, array_data)
  vctr_concat <- vctr_restore(c(4L, 5L, NA_integer_), vctr)

  stream <- narrow::as_narrow_array_stream(vctr_concat)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )

  array1 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array1), wk::xy(4, 8))
  array2 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array2), wk::xy(1, 5))
  array3 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array3), wk::xy(NA, NA))

  expect_null(narrow::narrow_array_stream_get_next(stream))
})

test_that("as_narrow_array_stream() works for geoarrow_vctr with >1 (scrambled) array", {
  vctr <- geoarrow(wk::xy(1:4, 5:8))
  schema <- attr(vctr, "schema")
  array_data <- attr(vctr, "array_data")[[1]]

  attr(vctr, "array_data") <- list(array_data, array_data)
  vctr_concat <- vctr_restore(c(5L, 4L), vctr)

  stream <- narrow::as_narrow_array_stream(vctr_concat)
  expect_identical(
    narrow::narrow_schema_info(
      narrow::narrow_array_stream_get_schema(stream),
      recursive = TRUE
    ),
    narrow::narrow_schema_info(schema, recursive = TRUE)
  )

  array1 <- narrow::narrow_array_stream_get_next(stream)
  expect_identical(wk::as_xy(array1), wk::xy(c(1, 4), c(5, 8)))
  expect_null(narrow::narrow_array_stream_get_next(stream))
})

test_that("c() works for geoarrow_vctr", {
  vctr1 <- geoarrow(wk::xy(1:3, 1:3, crs = "something"))
  vctr2 <- geoarrow_wkb(wk::xy(7:9, 7:9, crs = "something"))

  expect_identical(c(vctr1), vctr1)

  same_type <- c(vctr1, vctr1)
  expect_s3_class(same_type, "geoarrow_point")
  expect_identical(
    wk::as_xy(same_type),
    wk::xy(c(1:3, 1:3), c(1:3, 1:3), crs = "something")
  )
  # make sure we didn't copy any data
  same_type_out_data <- attr(same_type, "array_data")
  expect_length(same_type_out_data, 2)
  expect_identical(
    same_type_out_data,
    rep(attr(vctr1, "array_data"), 2)
  )

  diff_type <- c(vctr1, vctr2)
  expect_s3_class(diff_type, "geoarrow_wkb")
  expect_identical(
    wk::as_xy(diff_type),
    wk::xy(c(1:3, 7:9), c(1:3, 7:9), crs = "something")
  )

  expect_error(c(vctr1, wk::xy(crs = "something else")), "are not equal")
  expect_error(c(vctr1, wk::wkb(geodesic = TRUE)), "differing values for geodesic")
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
  attr(vctr, "array_data") <- list(array_data, array_data)
  vctr_concat <- vctr_restore(c(4L, 5L), vctr)
  expect_identical(
    wk::wk_handle(narrow::as_narrow_array(vctr_concat), wk::xy_writer()),
    wk::xy(c(4, 1), c(8, 5))
  )

  # zero-size arrays version
  attr(vctr, "array_data") <- list()
  vctr_concat <- vctr_restore(c(4L, 5L), vctr)
  expect_identical(
    wk::wk_handle(narrow::as_narrow_array(vctr_concat), wk::xy_writer()),
    wk::xy(c(NA, NA), c(NA, NA))
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
