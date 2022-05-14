
test_that("geoarrow_read_parquet/geoarrow_collect works", {
  skip_if_not(has_arrow_with_extension_type())

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  write_geoparquet(tbl, temp, schema = geoarrow_schema_wkb())

  table <- read_geoparquet(temp, handler = wk::xy_writer, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(read_geoparquet(temp, handler = wk::xy_writer)),
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
  skip_if_not(has_arrow_with_extension_type())

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52, crs = "EPSG:1234"))
  temp <- tempfile()
  write_geoparquet(tbl, temp)

  table <- arrow::read_parquet(temp, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))
  table$metadata$geo <- NULL

  expect_identical(
    as.data.frame(geoarrow_collect(table, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})

test_that("geoarrow_collect works without metadata or extension types", {
  skip_if_not(has_arrow_with_extension_type())

  geom_array <- geoarrow_create_narrow(wk::xy(1:26, 27:52))
  geom_array <- strip_extensions(geom_array)

  table <- arrow::arrow_table(
    id = letters,
    geom = narrow::from_narrow_array(geom_array, arrow::Array)
  )

  temp <- tempfile()
  arrow::write_parquet(table, temp)

  table2 <- arrow::read_parquet(temp, as_data_frame = FALSE)
  expect_true(inherits(table2, "Table"))
  expect_null(table$metadata$geo)

  expect_identical(
    as.data.frame(geoarrow_collect(table2, handler = wk::xy_writer)),
    data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  )

  unlink(temp)
})

test_that("geoarrow_collect works without extension types with metadata", {
  skip_if_not(has_arrow_with_extension_type())

  table <- as_geoarrow_table(
    data.frame(
      id = letters,
      geom = wk::xy(1:26, 27:52, crs = "EPSG:4326"),
      stringsAsFactors = FALSE
    ),
    geoparquet_metadata = TRUE
  )

  table$geom <- table$geom$chunk(0)$storage()

  temp <- tempfile()
  arrow::write_parquet(table, temp)

  table2 <- arrow::read_parquet(temp, as_data_frame = FALSE)
  expect_true(inherits(table2, "Table"))
  expect_false(is.null(table2$metadata$geo))

  expect_identical(
    as.data.frame(geoarrow_collect(table2, handler = wk::xy_writer)),
    data.frame(id = letters, geom = wk::xy(1:26, 27:52, crs = "EPSG:4326"))
  )

  collect_default <- geoarrow_collect(table2)
  expect_s3_class(collect_default$geom, "geoarrow_point")
  expect_identical(wk::wk_crs(collect_default$geom), "EPSG:4326")
  expect_identical(
    wk::as_xy(collect_default$geom),
    wk::xy(1:26, 27:52, crs = "EPSG:4326")
  )

  unlink(temp)
})

test_that("geoarrow_collect() works with zero columns", {
  skip_if_not(has_arrow_with_extension_type())

  table <- arrow::arrow_table(x = 1:5)[integer()]
  expect_equal(table$num_rows, 5)
  df <- geoarrow_collect(table)
  expect_equal(nrow(df), 5)
})

test_that("arrow::read|write_feather() just works with handleables", {
  skip_if_not(has_arrow_with_extension_type())

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  arrow::write_feather(tbl, temp)

  table <- arrow::read_feather(temp, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(geoarrow_collect(table, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})

test_that("geoarrow_collect() defaults to geoarrow_vctr representation", {
  skip_if_not(has_arrow_with_extension_type())

  table <- geoarrow_collect(
    arrow::arrow_table(x = 1:5, geom = wk::xy(1:5, 6:10))
  )

  expect_s3_class(table$geom, "geoarrow_point")
})

test_that("geoarrow_collect() works with data.frame", {
  expect_identical(
    geoarrow_collect(data.frame(a = 1, b = 2)),
    data.frame(a = 1, b = 2)
  )

  expect_identical(
    geoarrow_collect(
      data.frame(a = 1, b = wk::wkt("POINT (0 1)")),
      handler = wk::xy_writer()
    ),
    data.frame(a = 1, b = wk::xy(0, 1))
  )
})

test_that("arrow::read|write_ipc_stream() just works with handleables", {
  skip_if_not(has_arrow_with_extension_type())

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  arrow::write_ipc_stream(tbl, temp)

  table <- arrow::read_ipc_stream(temp, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(geoarrow_collect(table, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})

test_that("all example geoparquet files can be read", {
  skip_if_not(has_arrow_with_extension_type())

  files <- list.files(
    system.file("example_parquet", package = "geoarrow"),
    full.names = TRUE
  )

  for (file in files) {
    name <- gsub("-.*?\\.parquet$", "", basename(file))
    result <- read_geoparquet(
      file,
      handler = wk::wkb_writer()
    )

    if (startsWith(basename(file), "nc_spherical")) {
      # no nc_spherical in geoarrow_example_wkt
    } else if (grepl("^point", basename(file))) {
      # null points become empty when written to Parquet until
      # https://issues.apache.org/jira/browse/ARROW-8228 is resolved,
      # but wk::xy() represents them both the same way
      expect_identical(
        wk::as_xy(result$geometry),
        wk::as_xy(geoarrow_example_wkt[[!! name]])
      )
    } else {
      expect_identical(result$geometry, wk::as_wkb(geoarrow_example_wkt[[!! name]]))
    }
  }
})

test_that("all example feather files can be read", {
  skip_if_not(has_arrow_with_extension_type())

  files <- list.files(
    system.file("example_feather", package = "geoarrow"),
    full.names = TRUE
  )

  for (file in files) {
    name <- gsub("-.*?\\.feather$", "", basename(file))

    # check a more normal read
    result_table <- arrow::read_feather(
      file,
      as_data_frame = FALSE
    )

    result <- geoarrow::geoarrow_collect(result_table, handler = wk::wkb_writer)

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
      expect_identical(result$geometry, wk::as_wkb(geoarrow_example_wkt[[!! name]]))
    }
  }
})

test_that("all example ipc_stream files can be read", {
  skip_if_not(has_arrow_with_extension_type())

  files <- list.files(
    system.file("example_ipc_stream", package = "geoarrow"),
    full.names = TRUE
  )

  for (file in files) {
    name <- gsub("-.*?\\.arrows$", "", basename(file))

    # check a more normal read
    result_table <- arrow::read_ipc_stream(
      file,
      as_data_frame = FALSE
    )

    result <- geoarrow::geoarrow_collect(result_table, handler = wk::wkb_writer)

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
      expect_identical(result$geometry, wk::as_wkb(geoarrow_example_wkt[[!! name]]))
    }
  }
})
