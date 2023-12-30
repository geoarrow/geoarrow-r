
test_that("as_geoarrow_vctr() works for basic input", {
  vctr <- as_geoarrow_vctr(c("POINT (0 1)", "POINT (1 2)"))
  expect_identical(as.integer(unclass(vctr)), 1:2)
  expect_identical(as_geoarrow_vctr(vctr), vctr)

  expect_identical(infer_nanoarrow_schema(vctr)$format, "u")
  expect_identical(as_nanoarrow_schema(vctr)$format, "u")
  expect_identical(wk::as_wkt(vctr), wk::wkt(c("POINT (0 1)", "POINT (1 2)")))
})

test_that("format() works for geoarrow_vctr", {
  vctr <- as_geoarrow_vctr(c("POINT (0.222222 1.333333)", "POINT (1 2)"))
  expect_identical(
    format(vctr),
    c("<POINT (0.222222 1.333333)>", "<POINT (1 2)>")
  )
  expect_identical(
    as.character(vctr),
    c("<POINT (0.222222 1.333333)>", "<POINT (1 2)>")
  )

  opts <- options(digits = 5, width = 30)
  on.exit(options(opts))

  expect_identical(
    format(vctr),
    c("<POINT (0.22222 1.333>", "<POINT (1 2)>")
  )
})

test_that("wk crs/edge getters are implemented for geoarrow_vctr", {
  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)")))
  expect_identical(wk::wk_crs(x), NULL)
  expect_false(wk::wk_is_geodesic(x))

  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)"), crs = "EPSG:1234"))
  expect_identical(wk::wk_crs(x), "EPSG:1234")

  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)"), geodesic = TRUE))
  expect_true(wk::wk_is_geodesic(x))
})

test_that("geoarrow_vctr to stream generates an empty stream for empty slice", {
  vctr <- new_geoarrow_vctr(list(), na_extension_wkt())
  stream <- nanoarrow::as_nanoarrow_array_stream(vctr)
  schema_out <- stream$get_schema()
  expect_identical(schema_out$format, "u")
  expect_identical(nanoarrow::collect_array_stream(stream), list())
})

test_that("geoarrow_vctr to stream generates identical stream for identity slice", {
  array <- as_geoarrow_array("POINT (0 1)")
  vctr <- new_geoarrow_vctr(list(array), infer_nanoarrow_schema(array))

  stream <- nanoarrow::as_nanoarrow_array_stream(vctr)
  schema_out <- stream$get_schema()
  expect_identical(schema_out$format, "u")

  collected <- nanoarrow::collect_array_stream(stream)
  expect_length(collected, 1)
  expect_identical(
    nanoarrow::convert_buffer(array$buffers[[3]]),
    "POINT (0 1)"
  )
})

test_that("geoarrow_vctr to stream works for arbitrary slices", {
  array1 <- as_geoarrow_array(c("POINT (0 1)", "POINT (1 2)", "POINT (2 3)"))
  array2 <- as_geoarrow_array(
    c("POINT (4 5)", "POINT (5 6)", "POINT (6 7)", "POINT (7 8")
  )
  vctr <- new_geoarrow_vctr(list(array1, array2), infer_nanoarrow_schema(array1))

  chunks16 <- nanoarrow::collect_array_stream(
    nanoarrow::as_nanoarrow_array_stream(vctr[1:6])
  )
  expect_length(chunks16, 2)
  expect_identical(chunks16[[1]]$offset, 0L)
  expect_identical(chunks16[[1]]$length, 3L)
  expect_identical(chunks16[[2]]$offset, 0L)
  expect_identical(chunks16[[2]]$length, 3L)

  chunks34 <- nanoarrow::collect_array_stream(
    nanoarrow::as_nanoarrow_array_stream(vctr[3:4])
  )
  expect_length(chunks34, 2)
  expect_identical(chunks34[[1]]$offset, 2L)
  expect_identical(chunks34[[1]]$length, 1L)
  expect_identical(chunks34[[2]]$offset, 0L)
  expect_identical(chunks34[[2]]$length, 1L)

  chunks13 <- nanoarrow::collect_array_stream(
    nanoarrow::as_nanoarrow_array_stream(vctr[1:3])
  )
  expect_length(chunks13, 1)
  expect_identical(chunks13[[1]]$offset, 0L)
  expect_identical(chunks13[[1]]$length, 3L)

  chunks46 <- nanoarrow::collect_array_stream(
    nanoarrow::as_nanoarrow_array_stream(vctr[4:6])
  )
  expect_length(chunks46, 1)
  expect_identical(chunks46[[1]]$offset, 0L)
  expect_identical(chunks46[[1]]$length, 3L)

  chunks56 <- nanoarrow::collect_array_stream(
    nanoarrow::as_nanoarrow_array_stream(vctr[5:6])
  )
  expect_length(chunks56, 1)
  expect_identical(chunks56[[1]]$offset, 1L)
  expect_identical(chunks56[[1]]$length, 2L)

  chunks57 <- nanoarrow::collect_array_stream(
    nanoarrow::as_nanoarrow_array_stream(vctr[5:7])
  )
  expect_length(chunks57, 1)
  expect_identical(chunks57[[1]]$offset, 1L)
  expect_identical(chunks57[[1]]$length, 3L)
})

test_that("Errors occur for unsupported subset operations", {
  vctr <- as_geoarrow_vctr("POINT (0 1)")
  expect_error(
    vctr[5:1],
    "Can't subset geoarrow_vctr with non-slice"
  )

  expect_error(
    vctr[1] <- "something",
    "subset assignment for geoarrow_vctr is not supported"
  )

  expect_error(
    vctr[[1]] <- "something",
    "subset assignment for geoarrow_vctr is not supported"
  )
})

test_that("slice detector works", {
  expect_identical(
    vctr_as_slice(logical()),
    NULL
  )

  expect_identical(
    vctr_as_slice(2:1),
    NULL
  )

  expect_identical(
    vctr_as_slice(integer()),
    c(NA_integer_, 0L)
  )

  expect_identical(
    vctr_as_slice(2L),
    c(2L, 1L)
  )

  expect_identical(
    vctr_as_slice(1:10),
    c(1L, 10L)
  )

  expect_identical(
    vctr_as_slice(10:2048),
    c(10L, (2048L - 10L + 1L))
  )
})

test_that("chunk resolver works", {
  chunk_offset1 <- 0:10

  expect_identical(
    vctr_resolve_chunk(c(-1L, 11L), chunk_offset1),
    c(NA_integer_, NA_integer_)
  )

  expect_identical(
    vctr_resolve_chunk(9:0, chunk_offset1),
    9:0
  )
})
