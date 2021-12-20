
test_that("geoarrow_read_parquet works", {
  skip_if_not_installed("arrow")

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  write_geoarrow_parquet(tbl, temp, schema = geoarrow_schema_wkb())

  expect_identical(
    as.data.frame(read_geoarrow_parquet(temp, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})
