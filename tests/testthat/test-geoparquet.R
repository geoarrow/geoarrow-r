
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

test_that("geoparquet_column_spec_from_type() works", {
  # non-geoarrow type
  expect_identical(
    geoparquet_column_spec_from_type(nanoarrow::na_string()),
    list(encoding = "WKB", geometry_types = list())
  )

  # geoarrow type with add_geometry_types = FALSE
  expect_identical(
    geoparquet_column_spec_from_type(
      na_extension_geoarrow("POINT"),
      add_geometry_types = FALSE
    ),
    list(encoding = "WKB", geometry_types = list())
  )

  # geoarrow types with varying dimensions
  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("POINT")),
    list(encoding = "WKB", geometry_types = list("Point"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("POINT", "XYZ")),
    list(encoding = "WKB", geometry_types = list("Point Z"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("POINT", "XYM")),
    list(encoding = "WKB", geometry_types = list("Point M"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("POINT", "XYZM")),
    list(encoding = "WKB", geometry_types = list("Point ZM"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("POINT", "XYZ")),
    list(encoding = "WKB", geometry_types = list("Point Z"))
  )

  # Also check geometry t ypes
  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("LINESTRING")),
    list(encoding = "WKB", geometry_types = list("LineString"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("POLYGON")),
    list(encoding = "WKB", geometry_types = list("Polygon"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("MULTIPOINT")),
    list(encoding = "WKB", geometry_types = list("MultiPoint"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("MULTILINESTRING")),
    list(encoding = "WKB", geometry_types = list("MultiLineString"))
  )

  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("MULTIPOLYGON")),
    list(encoding = "WKB", geometry_types = list("MultiPolygon"))
  )

  # Check edge types
  expect_identical(
    geoparquet_column_spec_from_type(na_extension_geoarrow("LINESTRING", edges = "SPHERICAL")),
    list(encoding = "WKB", geometry_types = list("LineString"), edges = "spherical")
  )
})
