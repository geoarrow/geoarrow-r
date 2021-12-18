
test_that("geoarrow_write_parquet() works", {
  skip_if_not_installed("arrow")

  f <- tempfile(fileext = ".parquet")
  write_geoarrow_parquet(wk::xy(1:10, 11:20), f, schema = NULL)
  df <- arrow::read_parquet(f)
  expect_identical(names(df), "geometry")
  expect_identical(nrow(df), 10L)

  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  schema <- carrow::as_carrow_schema(table$schema)
  expect_identical(schema$children[[1]]$format, "+w:2")

  unlink(f)
})

test_that("geoarrow_write_parquet() works with explicit schema", {
  skip_if_not_installed("arrow")

  f <- tempfile(fileext = ".parquet")
  write_geoarrow_parquet(wk::xy(1:10, 11:20), f, schema = geoarrow_schema_wkb())
  df <- arrow::read_parquet(f)
  expect_identical(names(df), "geometry")
  expect_identical(nrow(df), 10L)

  table <- arrow::read_parquet(f, as_data_frame = FALSE)
  schema <- carrow::as_carrow_schema(table$schema)
  expect_identical(schema$children[[1]]$format, "w:21")

  unlink(f)
})

test_that("parquet_metadata_column() works for flat types", {
  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_wkt()),
    list(crs = NULL, encoding = "WKT")
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_wkt(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", encoding = "WKT")
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_wkt(geodesic = TRUE)),
    list(crs = NULL, geodesic = TRUE, encoding = "WKT")
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_wkb()),
    list(crs = NULL, encoding = "WKB")
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_geojson()),
    list(crs = NULL, encoding = "GeoJSON")
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_point()),
    list(crs = NULL, encoding = "point")
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_point(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", encoding = "point")
  )
})

test_that("parquet_metadata_column() works for linestring", {
  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_linestring()),
    list(
      crs = NULL,
      encoding = list(
        name = "linestring",
        point = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_linestring(geodesic = TRUE)),
    list(
      crs = NULL,
      geodesic = TRUE,
      encoding = list(
        name = "linestring",
        point = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    parquet_metadata_column(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      encoding = list(
        name = "linestring",
        point = list(
          encoding = "point"
        )
      )
    )
  )
})

test_that("parquet_metadata_column() works for polygon", {
  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_polygon()),
    list(
      crs = NULL,
      encoding = list(
        name = "polygon",
        point = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    parquet_metadata_column(geoarrow_schema_polygon(geodesic = TRUE)),
    list(
      crs = NULL,
      geodesic = TRUE,
      encoding = list(
        name = "polygon",
        point = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    parquet_metadata_column(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      encoding = list(
        name = "polygon",
        point = list(
          encoding = "point"
        )
      )
    )
  )
})

test_that("parquet_metadata_column() works for polygon", {
  expect_mapequal(
    parquet_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_point()
      )
    ),
    list(
      crs = NULL,
      encoding = list(
        name = "multi",
        child = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    parquet_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(geodesic = TRUE)
      )
    ),
    list(
      crs = NULL,
      geodesic = TRUE,
      encoding = list(
        name = "multi",
        child = list(
          encoding = list(
            name = "linestring",
            point = list(
              encoding = "point"
            )
          )
        )
      )
    )
  )

  expect_mapequal(
    parquet_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      encoding = list(
        name = "multi",
        child = list(
          encoding = "point"
        )
      )
    )
  )
})
