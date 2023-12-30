
test_that("as_geoarrow_array() for character() uses the wkt method", {
  array_chr <- as_geoarrow_array("POINT (0 1)")
  schema <- nanoarrow::infer_nanoarrow_schema(array_chr)
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkt")
})

test_that("as_geoarrow_array_stream() default method calls as_geoarrow_array()", {
  stream <- as_geoarrow_array_stream("POINT (0 1)")
  schema <- stream$get_schema()
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkt")
})

test_that("as_geoarrow_array() works for non-native geoarrow array", {
  array_wkt <- as_geoarrow_array(wk::wkt(c("POINT Z (0 1 2)", "POINT M (2 3 4)")))
  array <- as_geoarrow_array(array_wkt)

  skip("Test not implemented")
})

test_that("geoarrow_array_from_buffers() works for wkb", {
  wkb <- wk::as_wkb("POINT (0 1)")
  array <- geoarrow_array_from_buffers(
    na_extension_wkb(),
    list(
      NULL,
      c(0L, cumsum(lengths(unclass(wkb)))),
      wkb
    )
  )
  vctr <- nanoarrow::convert_array(force_array_storage(array))
  attributes(vctr) <- NULL
  attributes(wkb) <- NULL
  expect_identical(wkb, vctr)
})

test_that("geoarrow_array_from_buffers() works for large wkb", {
  skip_if_not_installed("arrow")

  wkb <- wk::as_wkb("POINT (0 1)")
  array <- geoarrow_array_from_buffers(
    na_extension_large_wkb(),
    list(
      NULL,
      c(0L, cumsum(lengths(unclass(wkb)))),
      wkb
    )
  )
  vctr <- nanoarrow::convert_array(force_array_storage(array))
  expect_identical(unclass(wkb), as.list(vctr))
})

test_that("geoarrow_array_from_buffers() works for wkt", {
  wkt <- "POINT (0 1)"
  array <- geoarrow_array_from_buffers(
    na_extension_wkt(),
    list(
      NULL,
      c(0L, cumsum(nchar(wkt))),
      wkt
    )
  )
  vctr <- nanoarrow::convert_array(force_array_storage(array), character())
  expect_identical(wkt, vctr)
})

test_that("geoarrow_array_from_buffers() works for large wkt", {
  skip_if_not_installed("arrow")

  wkt <- "POINT (0 1)"
  array <- geoarrow_array_from_buffers(
    na_extension_large_wkt(),
    list(
      NULL,
      c(0L, cumsum(nchar(wkt))),
      wkt
    )
  )
  vctr <- nanoarrow::convert_array(force_array_storage(array), character())
  expect_identical(wkt, vctr)
})

test_that("geoarrow_array_from_buffers() works for point", {
  array <- geoarrow_array_from_buffers(
    na_extension_geoarrow("POINT"),
    list(
      NULL,
      1:5,
      6:10
    )
  )

  expect_identical(
    as.raw(array$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(1:5)))
  )

  expect_identical(
    as.raw(array$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(6:10)))
  )
})

test_that("geoarrow_array_from_buffers() works for linestring", {
  array <- geoarrow_array_from_buffers(
    na_extension_geoarrow("LINESTRING"),
    list(
      NULL,
      c(0, 5),
      1:5,
      6:10
    )
  )

  expect_identical(
    as.raw(array$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(0L, 5L)))
  )

  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(1:5)))
  )

  expect_identical(
    as.raw(array$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(6:10)))
  )
})

test_that("geoarrow_array_from_buffers() works for multilinestring", {
  array <- geoarrow_array_from_buffers(
    na_extension_geoarrow("MULTILINESTRING"),
    list(
      NULL,
      c(0, 1),
      c(0, 5),
      1:5,
      6:10
    )
  )

  expect_identical(
    as.raw(array$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(0L, 1L)))
  )

  expect_identical(
    as.raw(array$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(0L, 5L)))
  )

  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(1:5)))
  )

  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(6:10)))
  )
})

test_that("geoarrow_array_from_buffers() works for multipolygon", {
  array <- geoarrow_array_from_buffers(
    na_extension_geoarrow("MULTIPOLYGON"),
    list(
      NULL,
      c(0, 1),
      c(0, 1),
      c(0, 5),
      1:5,
      6:10
    )
  )

  expect_identical(
    as.raw(array$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(0L, 1L)))
  )

  expect_identical(
    as.raw(array$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(0L, 1L)))
  )

  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(0L, 5L)))
  )

  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(1:5)))
  )

  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(as.double(6:10)))
  )
})
