
test_that("st_* methods work for geoarrow_vctr", {
  skip_if_not_installed("sf")

  vctr <- geoarrow(wk::wkt("POINT (0 1)"))
  expect_identical(sf::st_as_sfc(vctr), sf::st_as_sfc("POINT (0 1)"))
  expect_identical(sf::st_geometry(vctr), sf::st_as_sfc("POINT (0 1)"))
  expect_identical(sf::st_crs(vctr), sf::NA_crs_)
  expect_identical(
    sf::st_bbox(vctr),
    sf::st_bbox(sf::st_as_sfc("POINT (0 1)"))
  )
})

test_that("as_arrow_table() works for sf objects", {
  skip_if_not(has_arrow_extension_type())
  skip_if_not_installed("sf")

  tbl <- read_geoparquet_sf(
    system.file("example_parquet/point-geoarrow.parquet", package = "geoarrow")
  )

  table <- arrow::as_arrow_table(tbl)
  expect_s3_class(table, "Table")
  expect_s3_class(table$schema$geometry$type, "GeoArrowType")
  expect_null(table$r_metadata)

  metadata <- jsonlite::fromJSON(table$metadata$geo)
  expect_identical(metadata$columns$geometry$encoding, "geoarrow.point")
})


test_that("read_geoparquet_sf() works", {
  skip_if_not(has_arrow_extension_type())
  skip_if_not_installed("sf")

  tbl <- read_geoparquet_sf(
    system.file("example_parquet/point-wkb.parquet", package = "geoarrow"),
  )

  expect_s3_class(tbl, "sf")
  expect_identical(
    sf::st_as_text(tbl$geometry),
    c("POINT (30 10)", "POINT EMPTY", "POINT EMPTY")
  )
})

test_that("geoarrow_collect_sf() works", {
  skip_if_not(has_arrow_extension_type())
  skip_if_not_installed("sf")

  table <- arrow::read_parquet(
    system.file("example_parquet/point-wkb.parquet", package = "geoarrow"),
    as_data_frame = FALSE
  )

  tbl <- geoarrow_collect_sf(table)

  expect_s3_class(tbl, "sf")
  expect_identical(
    sf::st_as_text(tbl$geometry),
    c("POINT (30 10)", "POINT EMPTY", "POINT EMPTY")
  )
})
