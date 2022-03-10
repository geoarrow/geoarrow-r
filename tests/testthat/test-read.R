
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

  expect_identical(
    ds %>%
      dplyr::filter(id %in% c("a", "b", "c")) %>%
      geoarrow_collect(handler = wk::xy_writer, metadata = ds$metadata$geo) %>%
      as.data.frame(),
    tbl[1:3, ]
  )

  unlink(temp)
})

test_that("geoarrow_collect works without table-level metadata", {
  skip_if_not_installed("arrow")

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52, crs = "EPSG:1234"))
  temp <- tempfile()
  write_geoarrow_parquet(tbl, temp)

  table <- arrow::read_parquet(temp, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))
  table$metadata$geo <- NULL

  expect_identical(
    as.data.frame(geoarrow_collect(table, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})

test_that("geoarrow_read_feather() works", {
  skip_if_not_installed("arrow")

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  write_geoarrow_feather(tbl, temp, schema = geoarrow_schema_wkb())

  table <- read_geoarrow_feather(temp, handler = wk::xy_writer, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(read_geoarrow_feather(temp, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})

test_that("geoarrow_read_ipc_stream() works", {
  skip_if_not_installed("arrow")

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  write_geoarrow_ipc_stream(tbl, temp, schema = geoarrow_schema_wkb())

  table <- read_geoarrow_ipc_stream(temp, handler = wk::xy_writer, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(read_geoarrow_ipc_stream(temp, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})


test_that("all example parquet files can be read", {
  files <- list.files(
    system.file("example_parquet", package = "geoarrow"),
    full.names = TRUE
  )

  for (file in files) {
    if (grepl("^point.*?-default", basename(file))) {
      # currently fail because NULL values in a fixed-width list
      # aren't supported
      next
    }

    name <- gsub("-.*?\\.parquet$", "", basename(file))
    result <- read_geoarrow_parquet(
      file,
      handler = wk::wkt_writer()
    )

    if (startsWith(basename(file), "nc_spherical")) {
      # no nc_spherical in geoarrow_example_wkt
    } else if (grepl("^point", basename(file))) {
      # because an EMTPY point and a null point have different representations
      is_empty <- grepl("EMPTY$", geoarrow_example_wkt[[name]])
      expect_identical(
        wk::as_wkb(result$geometry[!is_empty]),
        wk::as_wkb(geoarrow_example_wkt[[!! name]][!is_empty])
      )
    } else {
      expect_identical(result$geometry, geoarrow_example_wkt[[!! name]])
    }
  }
})

test_that("all example feather files can be read", {
  files <- list.files(
    system.file("example_feather", package = "geoarrow"),
    full.names = TRUE
  )

  for (file in files) {
    name <- gsub("-.*?\\.feather$", "", basename(file))
    result <- read_geoarrow_feather(
      file,
      handler = wk::wkt_writer()
    )

    if (startsWith(basename(file), "nc_spherical")) {
      # no nc_spherical in geoarrow_example_wkt
    } else if (grepl("^point", basename(file))) {
      # because an EMTPY point and a null point have different representations
      is_empty <- grepl("EMPTY$", geoarrow_example_wkt[[name]])
      expect_identical(
        wk::as_wkb(result$geometry[!is_empty]),
        wk::as_wkb(geoarrow_example_wkt[[!! name]][!is_empty])
      )
    } else {
      expect_identical(result$geometry, geoarrow_example_wkt[[!! name]])
    }
  }
})

test_that("all example ipc_stream files can be read", {
  files <- list.files(
    system.file("example_ipc_stream", package = "geoarrow"),
    full.names = TRUE
  )

  for (file in files) {
    name <- gsub("-.*?\\.ipc$", "", basename(file))
    result <- read_geoarrow_ipc_stream(
      file,
      handler = wk::wkt_writer()
    )

    if (startsWith(basename(file), "nc_spherical")) {
      # no nc_spherical in geoarrow_example_wkt
    } else if (grepl("^point", basename(file))) {
      # because an EMTPY point and a null point have different representations
      is_empty <- grepl("EMPTY$", geoarrow_example_wkt[[name]])
      expect_identical(
        wk::as_wkb(result$geometry[!is_empty]),
        wk::as_wkb(geoarrow_example_wkt[[!! name]][!is_empty])
      )
    } else {
      expect_identical(result$geometry, geoarrow_example_wkt[[!! name]])
    }
  }
})
