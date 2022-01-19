
test_that("geoarrow_read_parquet/geoarrow_collect works", {
  skip_if_not_installed("arrow")

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  write_geoarrow_parquet(tbl, temp, schema = geoarrow_schema_wkb())

  table <- read_geoarrow_parquet(temp, handler = wk::xy_writer, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(read_geoarrow_parquet(temp, handler = wk::xy_writer)),
    tbl
  )

  ds <- arrow::open_dataset(temp)
  expect_identical(
    as.data.frame(geoarrow_collect(ds, handler = wk::xy_writer)),
    tbl
  )

  expect_error(
    ds %>%
      dplyr::filter(id %in% c("a", "b", "c")) %>%
      geoarrow_collect(),
    "`metadata` was not specified"
  )

  expect_identical(
    ds %>%
      dplyr::filter(id %in% c("a", "b", "c")) %>%
      geoarrow_collect(handler = wk::xy_writer, metadata = ds$metadata$geo) %>%
      as.data.frame(),
    tbl[1:3, ]
  )

  unlink(temp)
})
