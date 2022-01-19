
test_that("wk_handle() works for geoarrow.wkt", {
  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  arr <- geoarrow_create(src, schema = geoarrow_schema_wkt())
  expect_identical(wk::wk_handle(arr, wk::wkt_writer()), src)
})

test_that("wk_handle() works for geoarrow.wkt stream", {
  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  schema <- geoarrow_schema_wkt()

  # drip one element at a time
  stream_i <- 0
  stream_fun <- function() {
    if (stream_i >= length(src)) {
      return(NULL)
    }

    stream_i <<- stream_i + 1
    geoarrow_create(src[stream_i], schema = schema, strict = TRUE)
  }

  stream <- narrow::narrow_array_stream_function(schema, stream_fun)
  expect_identical(
    wk::wk_handle(stream, wk::wkt_writer()),
    src
  )
})

test_that("wk_handle() works for geoarrow.geojson", {
  skip_if_not_installed("geos")

  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  arr <- geoarrow_create(src, schema = geoarrow_schema_geojson())
  expect_identical(wk::wk_handle(arr, wk::wkt_writer()), src)
})

test_that("wk_handle() works for geoarrow.wkb", {
  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  arr <- geoarrow_create(src, schema = geoarrow_schema_wkb())
  expect_identical(wk::wk_handle(arr, wk::wkt_writer()), src)
})

test_that("wk_handle() works for geoarrow.wkb stream", {
  src <- wk::wkt(c("POINT (0 1)", "LINESTRING (1 1, 2 2)", NA))
  schema <- geoarrow_schema_wkb()

  # drip one element at a time
  stream_i <- 0
  stream_fun <- function() {
    if (stream_i >= length(src)) {
      return(NULL)
    }

    stream_i <<- stream_i + 1
    geoarrow_create(src[stream_i], schema = schema, strict = TRUE)
  }

  stream <- narrow::narrow_array_stream_function(schema, stream_fun)
  expect_identical(
    wk::wk_handle(stream, wk::wkt_writer()),
    src
  )
})

