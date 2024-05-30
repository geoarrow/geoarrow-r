
test_that("as_geoarrow_array() for character() uses the wkt method", {
  array_chr <- as_geoarrow_array("POINT (0 1)")
  schema <- nanoarrow::infer_nanoarrow_schema(array_chr)
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkt")
})

test_that("as_geoarrow_array() returns existing geoarrow array as-is", {
  array_wkt <- as_geoarrow_array(wk::wkt(c("POINT Z (0 1 2)", "POINT M (2 3 4)")))
  expect_identical(as_geoarrow_array(array_wkt), array_wkt)
})

test_that("as_geoarrow_array() can specify output schema", {
  array_wkt <- as_geoarrow_array(wk::wkt(c("POINT Z (0 1 2)", "POINT M (2 3 4)")))
  array <- as_geoarrow_array(array_wkt, schema = geoarrow_native("POINT", "XYZM"))
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.point")
  expect_identical(names(schema$children), c("x", "y", "z", "m"))
})

test_that("as_geoarrow_array() can create array from bare storage", {
  array_wkt <- nanoarrow::as_nanoarrow_array(c("POINT Z (0 1 2)", "POINT M (2 3 4)"))
  array <- as_geoarrow_array(array_wkt)
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkt")

  # Check that type + metadata from schema request is propagated
  array <- as_geoarrow_array(array_wkt, schema = geoarrow_wkb(edges = "SPHERICAL"))
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkb")
  expect_true(wk::wk_is_geodesic(as.vector(array)))
})

test_that("as_geoarrow_array_stream() default method calls as_geoarrow_array()", {
  stream <- as_geoarrow_array_stream("POINT (0 1)")
  schema <- stream$get_schema()
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkt")
})

test_that("as_geoarrow_array_stream() method for nanoarrow_array_stream works", {
  already_geoarrow_stream <- as_geoarrow_array_stream("POINT (0 1)")

  # No schema specified
  expect_identical(
    as_geoarrow_array_stream(already_geoarrow_stream),
    already_geoarrow_stream
  )

  # Same schema specified
  expect_identical(
    as_geoarrow_array_stream(already_geoarrow_stream, schema = na_extension_wkt()),
    already_geoarrow_stream
  )

  # Different schema specified
  stream <- as_geoarrow_array_stream(already_geoarrow_stream, schema = na_extension_wkb())
  schema <- stream$get_schema()
  expect_identical(
    schema$metadata[["ARROW:extension:name"]],
    "geoarrow.wkb"
  )

  expect_identical(
    wk::as_wkt(as_geoarrow_vctr(stream)),
    wk::wkt("POINT (0 1)")
  )

  # Bare storage
  array_wkt <- nanoarrow::as_nanoarrow_array(c("POINT Z (0 1 2)", "POINT M (2 3 4)"))
  stream_wkt <- nanoarrow::as_nanoarrow_array_stream(array_wkt)
  stream <- as_geoarrow_array_stream(stream_wkt)
  schema <- nanoarrow::infer_nanoarrow_schema(stream)
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkt")

  # Check that type + metadata from schema request is propagated
  stream_wkt <- nanoarrow::as_nanoarrow_array_stream(array_wkt)
  stream <- as_geoarrow_array_stream(
    stream_wkt,
    schema = geoarrow_wkb(edges = "SPHERICAL")
  )
  schema <- nanoarrow::infer_nanoarrow_schema(stream)
  expect_identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkb")
  expect_true(wk::wk_is_geodesic(as.vector(stream)))
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

test_that("geoarrow_array_from_buffers() works for empty wkb", {
  array <- geoarrow_array_from_buffers(
    na_extension_wkb(),
    list(
      NULL,
      NULL,
      raw()
    )
  )
  vctr <- nanoarrow::convert_array(force_array_storage(array))
  attributes(vctr) <- NULL
  expect_identical(list(), vctr)
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

test_that("geoarrow_array_from_buffers() works for interleaved point", {
  array <- geoarrow_array_from_buffers(
    na_extension_geoarrow("POINT", coord_type = "INTERLEAVED"),
    list(
      NULL,
      rbind(1:5, 6:10)
    )
  )

  expect_identical(
    as.raw(array$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 6, 2, 7, 3, 8, 4, 9, 5, 10)))
  )
})

test_that("geoarrow_array_from_buffers() works for empty point", {
  array <- geoarrow_array_from_buffers(
    na_extension_geoarrow("POINT"),
    list(
      NULL,
      double(),
      double()
    )
  )

  expect_identical(array$length, 0L)
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

test_that("geoarrow_array_from_buffers() works for empty linestring", {
  array <- geoarrow_array_from_buffers(
    na_extension_geoarrow("LINESTRING"),
    list(
      NULL,
      NULL,
      double(),
      double()
    )
  )

  expect_identical(array$length, 0L)
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

test_that("binary buffers can be created", {
  # raw
  buffer <- as_binary_buffer(as.raw(1:5))
  expect_identical(as.raw(buffer), as.raw(1:5))

  # buffer
  expect_identical(as_binary_buffer(buffer), buffer)

  # string
  expect_identical(
    as.raw(as_binary_buffer(c("abc", "def"))),
    charToRaw("abcdef")
  )

  # list
  expect_identical(
    as.raw(as_binary_buffer(list(as.raw(1:5)))),
    as.raw(1:5)
  )

  expect_error(
    as_binary_buffer(new.env()),
    "Don't know how to create binary data buffer"
  )
})

test_that("coord buffers can be created", {
  buffer <- as_coord_buffer(c(1, 2, 3))
  expect_identical(
    nanoarrow::convert_buffer(buffer),
    c(1, 2, 3)
  )

  expect_identical(as_coord_buffer(buffer), buffer)
})

test_that("offset buffers can be created", {
  buffer <- as_offset_buffer(c(1, 2, 3))
  expect_identical(
    nanoarrow::convert_buffer(buffer),
    c(1L, 2L, 3L)
  )

  expect_identical(as_offset_buffer(buffer), buffer)
})

test_that("validity buffers can be created", {
  validity <- as_validity_buffer(NULL)
  expect_identical(validity$null_count, 0L)
  expect_identical(as.raw(validity$buffer), raw())

  validity <- as_validity_buffer(c(TRUE, FALSE, TRUE))
  expect_identical(validity$null_count, 1L)
  expect_identical(
    nanoarrow::convert_buffer(validity$buffer)[1:3],
    c(TRUE, FALSE, TRUE)
  )

  validity <- as_validity_buffer(validity$buffer)
  expect_identical(
    validity$null_count,
    -1L
  )
  expect_identical(
    nanoarrow::convert_buffer(validity$buffer)[1:3],
    c(TRUE, FALSE, TRUE)
  )

  expect_error(
    as_validity_buffer(c(TRUE, FALSE, NA)),
    "NA values are not allowed in validity buffer"
  )
})
