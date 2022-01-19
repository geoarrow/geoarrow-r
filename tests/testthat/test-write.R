
test_that("geoarrow_write_parquet() works", {
  skip_if_not_installed("arrow")

  f <- tempfile(fileext = ".parquet")
  write_geoarrow_parquet(wk::xy(1:10, 11:20), f, schema = NULL)
  df <- arrow::read_parquet(f)
  expect_identical(names(df), "geometry")
  expect_identical(nrow(df), 10L)

  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  schema <- narrow::as_narrow_schema(table$schema)
  expect_identical(schema$children[[1]]$format, "+w:2")

  unlink(f)
})

test_that("geoarrow_write_parquet() roundtrips metadata", {
  skip_if_not_installed("arrow")

  f <- tempfile(fileext = ".parquet")
  write_geoarrow_parquet(data.frame(col = wk::xy(1:10, 11:20)), f, schema = NULL)
  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  meta <- jsonlite::fromJSON(table$metadata$geo)
  expect_identical(meta$primary_column, "col")
  expect_identical(meta$columns$col$encoding, "point")
})

test_that("geoarrow_write_parquet() works with explicit schema", {
  skip_if_not_installed("arrow")

  f <- tempfile(fileext = ".parquet")
  write_geoarrow_parquet(wk::xy(1:10, 11:20), f, schema = geoarrow_schema_wkb())
  df <- arrow::read_parquet(f)
  expect_identical(names(df), "geometry")
  expect_identical(nrow(df), 10L)

  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  schema <- narrow::as_narrow_schema(table$schema)
  expect_identical(schema$children[[1]]$format, "w:21")

  unlink(f)
})
