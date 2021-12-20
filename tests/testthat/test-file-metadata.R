
test_that("file_metadata_table() works", {
  expect_error(file_metadata_table(carrow::carrow_schema("i")), "zero columns")

  schema <- carrow::carrow_schema(
    format = "+s",
    children = list(
      geoarrow_schema_wkb(name = "col1"),
      geoarrow_schema_wkt(name = "col2")
    )
  )

  meta <- file_metadata_table(schema)
  expect_identical(meta$primary_column, "col1")
  expect_identical(meta$columns$col1$encoding, "WKB")
  expect_identical(meta$columns$col2$encoding, "WKT")

  meta2 <- file_metadata_table(schema, primary_column = "col2")
  expect_identical(meta2$primary_column, "col2")
})

test_that("file_metadata_column() works for flat types", {
  expect_mapequal(
    file_metadata_column(geoarrow_schema_wkt()),
    list(crs = NULL, encoding = "WKT")
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_wkt(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", encoding = "WKT")
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_wkt(geodesic = TRUE)),
    list(crs = NULL, geodesic = TRUE, encoding = "WKT")
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_wkb()),
    list(crs = NULL, encoding = "WKB")
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_geojson()),
    list(crs = NULL, encoding = "GeoJSON")
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_point(dim = "xyz")),
    list(crs = NULL, dim = "xyz", encoding = "point")
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_point(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", dim = "xy", encoding = "point")
  )
})

test_that("file_metadata_column() works for linestring", {
  expect_mapequal(
    file_metadata_column(geoarrow_schema_linestring()),
    list(
      crs = NULL,
      dim = "xy",
      encoding = list(
        name = "linestring",
        point = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_linestring(geodesic = TRUE)),
    list(
      crs = NULL,
      dim = "xy",
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
    file_metadata_column(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      dim = "xy",
      encoding = list(
        name = "linestring",
        point = list(
          encoding = "point"
        )
      )
    )
  )
})

test_that("file_metadata_column() works for polygon", {
  expect_mapequal(
    file_metadata_column(geoarrow_schema_polygon()),
    list(
      crs = NULL,
      dim = "xy",
      encoding = list(
        name = "polygon",
        point = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    file_metadata_column(geoarrow_schema_polygon(geodesic = TRUE)),
    list(
      crs = NULL,
      dim = "xy",
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
    file_metadata_column(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      dim = "xy",
      encoding = list(
        name = "polygon",
        point = list(
          encoding = "point"
        )
      )
    )
  )
})

test_that("file_metadata_column() works for polygon", {
  expect_mapequal(
    file_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_point()
      )
    ),
    list(
      crs = NULL,
      dim = "xy",
      encoding = list(
        name = "multi",
        child = list(
          encoding = "point"
        )
      )
    )
  )

  expect_mapequal(
    file_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(geodesic = TRUE)
      )
    ),
    list(
      crs = NULL,
      dim = "xy",
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
    file_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      dim = "xy",
      encoding = list(
        name = "multi",
        child = list(
          encoding = "point"
        )
      )
    )
  )
})

test_that("schema_from_column_metadata() works for WKT", {
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKT"),
    carrow::carrow_schema(name = "", format = "u")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkt(nullable = FALSE)
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "WKT"),
    carrow::carrow_schema(format = "u", name = "")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkt(crs = "EPSG:1234", nullable = FALSE)
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(geodesic = TRUE, encoding = "WKT"),
    carrow::carrow_schema(format = "u", name = "")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkt(geodesic = TRUE, nullable = FALSE)
    )
  )

  # nullable
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKT"),
    carrow::carrow_schema(
      format = "u",
      name = "",
      flags = carrow::carrow_schema_flags(nullable = TRUE)
    )
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkt(format = "u", nullable = TRUE)
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKT"),
    carrow::carrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkt(format = "w:12", nullable = FALSE)
    )
  )
})

test_that("schema_from_column_metadata() works for WKB", {
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKB"),
    carrow::carrow_schema(name = "", format = "z")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkb(nullable = FALSE)
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "WKB"),
    carrow::carrow_schema(format = "z", name = "")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkb(crs = "EPSG:1234", nullable = FALSE)
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(geodesic = TRUE, encoding = "WKB"),
    carrow::carrow_schema(format = "z", name = "")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkb(geodesic = TRUE, nullable = FALSE)
    )
  )

  # nullable
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKB"),
    carrow::carrow_schema(
      format = "z",
      name = "",
      flags = carrow::carrow_schema_flags(nullable = TRUE)
    )
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkb(format = "z", nullable = TRUE)
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKB"),
    carrow::carrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_wkb(format = "w:12", nullable = FALSE)
    )
  )
})

test_that("schema_from_column_metadata() works for GeoJSON", {
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "GeoJSON"),
    carrow::carrow_schema(name = "", format = "u")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_geojson(nullable = FALSE)
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "GeoJSON"),
    carrow::carrow_schema(format = "u", name = "")
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_geojson(crs = "EPSG:1234", nullable = FALSE)
    )
  )

  # nullable
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "GeoJSON"),
    carrow::carrow_schema(
      format = "u",
      name = "",
      flags = carrow::carrow_schema_flags(nullable = TRUE)
    )
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_geojson(format = "u", nullable = TRUE)
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "GeoJSON"),
    carrow::carrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed),
    carrow::carrow_schema_info(
      geoarrow_schema_geojson(format = "w:12", nullable = FALSE)
    )
  )
})

test_that("schema_from_column_metadata() works for point", {
  bare_point <- geoarrow_schema_point(dim = "xy")
  bare_point$metadata <- NULL
  bare_point$flags <- 0L

  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point(nullable = FALSE),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point(nullable = FALSE, crs = "EPSG:1234"),
      recursive = TRUE
    )
  )

  # with dim
  bare_point$format <- "w:4"
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point", dim = "xyzm"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point(nullable = FALSE, dim = "xyzm", crs = "EPSG:1234"),
      recursive = TRUE
    )
  )

  # nullable
  bare_point$flags <- carrow::carrow_schema_flags(nullable = TRUE)
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point(nullable = TRUE),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for point struct", {
  bare_point <- geoarrow_schema_point_struct(dim = "xy")
  bare_point$metadata <- NULL
  bare_point$flags <- 0L

  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point_struct(nullable = FALSE),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point_struct(nullable = FALSE, crs = "EPSG:1234"),
      recursive = TRUE
    )
  )

  # nullable
  bare_point$flags <- carrow::carrow_schema_flags(nullable = TRUE)
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point_struct(nullable = TRUE),
      recursive = TRUE
    )
  )

  # with dim
  bare_point$children <- lapply(1:4, function(x) carrow::carrow_schema("g"))
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point", dim = "xyzm"),
    bare_point
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_point_struct(dim = "xyzm", crs = "EPSG:1234"),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for linestring", {
  bare <- geoarrow_schema_linestring()
  bare$metadata <- NULL
  bare$flags <- 0L

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = list(name = "linestring", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_linestring(nullable = FALSE),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
      encoding = list(name = "linestring", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(crs = "EPSG:1234", nullable = FALSE),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
      encoding = list(name = "linestring", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(dim = "xyzm", nullable = FALSE),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, geodesic = TRUE, dim = "xy",
      encoding = list(name = "linestring", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_linestring(
        geodesic = TRUE,
        point = geoarrow_schema_point(nullable = FALSE),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # nullable
  bare$flags <- carrow::carrow_schema_flags(nullable = TRUE)
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = list(name = "linestring", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_linestring(nullable = TRUE),
      recursive = TRUE
    )
  )

  # non-default storage type
  bare$format <- "+L"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = list(name = "linestring", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_linestring(nullable = TRUE, format = "+L"),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for polygon", {
  bare <- geoarrow_schema_polygon()
  bare$metadata <- NULL
  bare$flags <- 0L

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = list(name = "polygon", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_polygon(nullable = FALSE),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
      encoding = list(name = "polygon", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(crs = "EPSG:1234", nullable = FALSE),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
      encoding = list(name = "polygon", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(dim = "xyzm", nullable = FALSE),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, geodesic = TRUE, dim = "xy",
      encoding = list(name = "polygon", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_polygon(
        geodesic = TRUE,
        point = geoarrow_schema_point(nullable = FALSE),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # nullable
  bare$flags <- carrow::carrow_schema_flags(nullable = TRUE)
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = list(name = "polygon", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_polygon(nullable = TRUE),
      recursive = TRUE
    )
  )

  # non-default storage type
  bare$format <- "+L"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = list(name = "polygon", point = list(encoding = "point"))
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_polygon(nullable = TRUE, format = c("+L", "+l")),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for multi", {
  bare <- geoarrow_schema_multi(geoarrow_schema_linestring())
  bare$metadata <- NULL
  bare$children[[1]]$metadata <- NULL
  bare$children[[1]]$flags <- 0L
  bare$children[[1]]$children[[1]]$metadata <- NULL
  bare$flags <- 0L

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
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
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(nullable = FALSE),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
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
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(
          nullable = FALSE,
          point = geoarrow_schema_point(crs = "EPSG:1234", nullable = FALSE)
        ),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
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
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(
          nullable = FALSE,
          point = geoarrow_schema_point(dim = "xyzm", nullable = FALSE)
        ),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy", geodesic = TRUE,
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
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(
          nullable = FALSE,
          geodesic = TRUE
        ),
        nullable = FALSE
      ),
      recursive = TRUE
    )
  )

  # nullable
  bare$flags <- carrow::carrow_schema_flags(nullable = TRUE)
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
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
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(nullable = FALSE)
      ),
      recursive = TRUE
    )
  )

  # non-default storage type
  bare$format <- "+L"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
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
    ),
    bare
  )
  expect_identical(
    carrow::carrow_schema_info(schema_reconstructed, recursive = TRUE),
    carrow::carrow_schema_info(
      geoarrow_schema_multi(
        format = "+L",
        geoarrow_schema_linestring(nullable = FALSE)
      ),
      recursive = TRUE
    )
  )
})
