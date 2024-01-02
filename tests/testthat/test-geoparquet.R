
test_that("write_geoparquet_table() can write a data.frame", {
  skip_if_not(has_geoparquet_dependencies())

  tmp <- tempfile()
  on.exit(unlink(tmp))

  df <- data.frame(not_geometry = 1L, geometry = wk::wkt("POINT (0 1)"))
  write_geoparquet(df, tmp)
  table <- arrow::read_parquet(tmp, as_data_frame = FALSE)

  expect_true("geo" %in% names(table$metadata))
  geo_meta <- jsonlite::fromJSON(table$metadata$geo, simplifyVector = FALSE)
  expect_identical(geo_meta$version, "1.0.0")
  expect_identical(geo_meta$primary_geometry_column, "geometry")
  expect_identical(
    geo_meta$columns$geometry,
    list(encoding = "WKB", geometry_types = list())
  )

  expect_true(table$schema$geometry$type$Equals(arrow::binary()))
  wkb <- wk::wkb(as.vector(table$geometry))
  expect_identical(wk::as_wkt(wkb), wk::wkt("POINT (0 1)"))
})
