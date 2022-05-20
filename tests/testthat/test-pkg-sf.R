
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

test_that("st_geometry() methods work for Arrow table-like things", {
  skip_if_not_installed("sf")
  skip_if_not(has_arrow_with_extension_type())

  vctr <- geoarrow(wk::wkt("POINT (0 1)", crs = "OGC:CRS84"))
  table <- arrow::arrow_table(geom = vctr)
  batch <- arrow::record_batch(geom = vctr)
  dataset <- arrow::InMemoryDataset$create(table)
  query <- dplyr::filter(table, arrow::Expression$scalar(TRUE))

  expect_identical(
    sf::st_geometry(table),
    sf::st_as_sfc("POINT (0 1)", crs = sf::st_crs("OGC:CRS84"))
  )

  expect_identical(
    sf::st_geometry(batch),
    sf::st_as_sfc("POINT (0 1)", crs = sf::st_crs("OGC:CRS84"))
  )

  expect_identical(
    sf::st_geometry(dataset),
    sf::st_as_sfc("POINT (0 1)", crs = sf::st_crs("OGC:CRS84"))
  )

  expect_identical(
    sf::st_geometry(query),
    sf::st_as_sfc("POINT (0 1)", crs = sf::st_crs("OGC:CRS84"))
  )
})

test_that("st_crs() methods work for Arrow table-like things", {
  skip_if_not_installed("sf")
  skip_if_not(has_arrow_with_extension_type())

  vctr <- geoarrow(wk::wkt("POINT (0 1)", crs = "OGC:CRS84"))
  table <- arrow::arrow_table(geom = vctr)
  batch <- arrow::record_batch(geom = vctr)
  dataset <- arrow::InMemoryDataset$create(table)
  query <- dplyr::filter(table, arrow::Expression$scalar(TRUE))

  expect_identical(
    sf::st_crs(table),
    sf::st_crs("OGC:CRS84")
  )

  expect_identical(
    sf::st_crs(batch),
    sf::st_crs("OGC:CRS84")
  )

  expect_identical(
    sf::st_crs(dataset),
    sf::st_crs("OGC:CRS84")
  )

  expect_identical(
    sf::st_crs(query),
    sf::st_crs("OGC:CRS84")
  )
})


test_that("st_crs() methods work for Arrow table-like things", {
  skip_if_not_installed("sf")
  skip_if_not(has_arrow_with_extension_type())

  vctr <- geoarrow(wk::wkt("POINT (0 1)", crs = "OGC:CRS84"))
  table <- arrow::arrow_table(geom = vctr)
  batch <- arrow::record_batch(geom = vctr)
  dataset <- arrow::InMemoryDataset$create(table)
  query <- dplyr::filter(table, arrow::Expression$scalar(TRUE))

  bbox <- sf::st_bbox(wk::rct(0, 1, 0, 1, crs = "OGC:CRS84"))

  expect_identical(
    sf::st_bbox(table),
    bbox
  )

  expect_identical(
    sf::st_bbox(batch),
    bbox
  )

  expect_identical(
    sf::st_bbox(dataset),
    bbox
  )

  expect_identical(
    sf::st_bbox(query),
    bbox
  )
})

test_that("st_as_sf() methods work for Arrow table-like things", {
  skip_if_not_installed("sf")
  skip_if_not(has_arrow_with_extension_type())

  vctr <- geoarrow(wk::wkt("POINT (0 1)", crs = "OGC:CRS84"))
  table <- arrow::arrow_table(geometry = vctr)
  batch <- arrow::record_batch(geometry = vctr)
  dataset <- arrow::InMemoryDataset$create(table)
  query <- dplyr::filter(table, arrow::Expression$scalar(TRUE))

  sf_tbl <- sf::st_as_sf(
    data.frame(
      geom = sf::st_as_sfc("POINT (0 1)", crs = sf::st_crs("OGC:CRS84"))
    )
  )

  expect_identical(
    sf::st_as_sf(table),
    sf_tbl
  )

  expect_identical(
    sf::st_as_sf(batch),
    sf_tbl
  )

  expect_equal(
    sf::st_as_sf(dataset),
    sf_tbl,
    ignore_attr = TRUE
  )

  expect_identical(
    sf::st_as_sf(query),
    sf_tbl
  )
})

test_that("geoarrow_collect_sf() works on a data.frame", {
  skip_if_not_installed("sf")

  df <- data.frame(x = 1, geometry = as_geoarrow(wk::wkt("POINT (30 10)")))
  expect_identical(
    geoarrow_collect_sf(df),
    sf::st_as_sf(
      data.frame(x = 1, geometry = sf::st_as_sfc("POINT (30 10)"))
    )
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
