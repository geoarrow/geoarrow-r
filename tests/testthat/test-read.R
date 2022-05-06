
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

test_that("geoarrow_read_feather() works", {
  skip_if_not(has_arrow_with_extension_type())

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  write_geoparquet_feather(tbl, temp, schema = geoarrow_schema_wkb())

  table <- read_geoparquet_feather(temp, handler = wk::xy_writer, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(read_geoparquet_feather(temp, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})

test_that("geoarrow_read_ipc_stream() works", {
  skip_if_not(has_arrow_with_extension_type())

  tbl <- data.frame(id = letters, geom = wk::xy(1:26, 27:52))
  temp <- tempfile()
  write_geoparquet_ipc_stream(tbl, temp, schema = geoarrow_schema_wkb())

  table <- read_geoparquet_ipc_stream(temp, handler = wk::xy_writer, as_data_frame = FALSE)
  expect_true(inherits(table, "Table"))

  expect_identical(
    as.data.frame(read_geoparquet_ipc_stream(temp, handler = wk::xy_writer)),
    tbl
  )

  unlink(temp)
})


test_that("all example parquet files can be read", {
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

    # check the metadata
    result <- read_geoparquet_feather(
      file,
      as_data_frame = FALSE
    )
    metadata <- jsonlite::fromJSON(result$metadata$geo)

    # all examples so far are single geometry type
    expect_length(metadata$columns$geometry$geometry_type, 1)
    geometry_type_split <- strsplit(
      metadata$columns$geometry$geometry_type,
      " "
    )[[1]]
    expect_true(
      geometry_type_split[1] %in%
        c("Point", "LineString", "Polygon",
          "MultiPoint", "MultiLineString", "MultiPolygon",
          "GeometryCollection")
    )

    expect_true(
      is.na(geometry_type_split[2]) ||
        geometry_type_split[2] %in% c("Z", "M", "ZM")
    )

    # check a more normal read
    result <- read_geoparquet_feather(
      file,
      handler = wk::wkb_writer()
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
    result <- read_geoparquet_ipc_stream(
      file,
      handler = wk::wkb_writer()
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
      expect_identical(result$geometry, wk::as_wkb(geoarrow_example_wkt[[!! name]]))
    }
  }
})
