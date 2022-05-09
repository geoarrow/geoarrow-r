
test_that("geoarrow_write_parquet() works", {
  skip_if_not(has_arrow_with_extension_type())

  f <- tempfile(fileext = ".parquet")
  write_geoparquet(wk::xy(1:10, 11:20), f, schema = NULL)
  df <- arrow::read_parquet(f)
  expect_identical(names(df), "geometry")
  expect_identical(nrow(df), 10L)

  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  schema <- narrow::as_narrow_schema(table$schema)
  expect_identical(schema$children[[1]]$format, "z")
  expect_identical(
    schema$children[[1]]$metadata[["ARROW:extension:name"]],
    "geoarrow.wkb"
  )

  unlink(f)
})

test_that("write_geoparquet() writes null points that can be read again", {
  skip_if_not(has_arrow_with_extension_type())

  f <- tempfile(fileext = ".parquet")
  features <- wk::wkt(c("POINT (1 3)", "POINT (2 4)", NA))
  write_geoparquet(features, f, schema = geoarrow_schema_point())
  df <- read_geoparquet(f, handler = wk::wkt_writer())
  expect_identical(
    df[[1]],
    wk::wkt(c("POINT (1 3)", "POINT (2 4)", "POINT (nan nan)"))
  )

  unlink(f)
})

test_that("geoarrow_write_parquet() roundtrips metadata", {
  skip_if_not(has_arrow_with_extension_type())

  f <- tempfile(fileext = ".parquet")
  write_geoparquet(data.frame(col = wk::xy(1:10, 11:20)), f, schema = NULL)
  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  meta <- jsonlite::fromJSON(table$metadata$geo)
  expect_identical(meta$primary_column, "col")
  expect_identical(meta$columns$col$encoding, "WKB")
  expect_identical(meta$columns$col$geometry_type, "Point")
  expect_equal(meta$columns$col$bbox, c(1, 11, 10, 20))
})

test_that("write_geoparquet() works with explicit schema", {
  skip_if_not(has_arrow_with_extension_type())

  f <- tempfile(fileext = ".parquet")
  write_geoparquet(wk::xy(1:10, 11:20), f, schema = geoarrow_schema_wkb())
  df <- arrow::read_parquet(f)
  expect_identical(names(df), "geometry")
  expect_identical(nrow(df), 10L)

  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  schema <- narrow::as_narrow_schema(table$schema)
  expect_identical(schema$children[[1]]$format, "z")

  unlink(f)
})
