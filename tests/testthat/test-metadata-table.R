
test_that("geoarrow_metadata_table() works", {
  expect_error(geoarrow_metadata_table(narrow::narrow_schema("i")), "zero columns")

  schema <- narrow::narrow_schema(
    format = "+s",
    children = list(
      geoarrow_schema_wkb(name = "col1"),
      geoarrow_schema_wkt(name = "col2")
    )
  )

  meta <- geoarrow_metadata_table(schema)
  expect_identical(meta$primary_column, "col1")
  expect_identical(meta$columns$col1$encoding, "WKB")
  expect_identical(meta$columns$col2$encoding, "WKT")

  meta2 <- geoarrow_metadata_table(schema, primary_column = "col2")
  expect_identical(meta2$primary_column, "col2")
})

test_that("geoarrow_metadata_column() works for flat types", {
  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_wkt()),
    list(crs = NULL, encoding = "WKT")
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_wkt(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", encoding = "WKT")
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_wkt(geodesic = TRUE)),
    list(crs = NULL, geodesic = TRUE, encoding = "WKT")
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_wkb()),
    list(crs = NULL, encoding = "WKB")
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_geojson()),
    list(crs = NULL, encoding = "GeoJSON")
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_point(dim = "xyz")),
    list(crs = NULL, dim = "xyz", encoding = "point")
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_point(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", dim = "xy", encoding = "point")
  )
})

test_that("geoarrow_metadata_column() works for linestring", {
  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_linestring()),
    list(
      crs = NULL,
      dim = "xy",
      encoding = "linestring"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_linestring(geodesic = TRUE)),
    list(
      crs = NULL,
      dim = "xy",
      geodesic = TRUE,
      encoding = "linestring"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      dim = "xy",
      encoding = "linestring"
    )
  )
})

test_that("geoarrow_metadata_column() works for polygon", {
  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_polygon()),
    list(
      crs = NULL,
      dim = "xy",
      encoding = "polygon"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_polygon(geodesic = TRUE)),
    list(
      crs = NULL,
      dim = "xy",
      geodesic = TRUE,
      encoding = "polygon"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      dim = "xy",
      encoding = "polygon"
    )
  )
})

test_that("geoarrow_metadata_column() works for polygon", {
  expect_mapequal(
    geoarrow_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_point()
      )
    ),
    list(
      crs = NULL,
      dim = "xy",
      encoding = "multipoint"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(geodesic = TRUE)
      )
    ),
    list(
      crs = NULL,
      dim = "xy",
      geodesic = TRUE,
      encoding = "multilinestring"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(
      geoarrow_schema_multi(
        geoarrow_schema_point(crs = "EPSG:1234")
      )
    ),
    list(
      crs = "EPSG:1234",
      dim = "xy",
      encoding = "multipoint"
    )
  )
})

test_that("schema_from_column_metadata() works for WKT", {
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKT"),
    narrow::narrow_schema(name = "", format = "u")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt()
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "WKT"),
    narrow::narrow_schema(format = "u", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(crs = "EPSG:1234", )
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(geodesic = TRUE, encoding = "WKT"),
    narrow::narrow_schema(format = "u", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(geodesic = TRUE)
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKT"),
    narrow::narrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkt(format = "w:12")
    )
  )
})

test_that("schema_from_column_metadata() works for WKB", {
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKB"),
    narrow::narrow_schema(name = "", format = "z")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb()
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "WKB"),
    narrow::narrow_schema(format = "z", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb(crs = "EPSG:1234")
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(geodesic = TRUE, encoding = "WKB"),
    narrow::narrow_schema(format = "z", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb(geodesic = TRUE)
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "WKB"),
    narrow::narrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_wkb(format = "w:12")
    )
  )
})

test_that("schema_from_column_metadata() works for GeoJSON", {
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "GeoJSON"),
    narrow::narrow_schema(name = "", format = "u")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_geojson()
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "GeoJSON"),
    narrow::narrow_schema(format = "u", name = "")
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_geojson(crs = "EPSG:1234")
    )
  )

  # non-default storage type
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "GeoJSON"),
    narrow::narrow_schema(
      format = "w:12",
      name = ""
    )
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed),
    narrow::narrow_schema_info(
      geoarrow_schema_geojson(format = "w:12")
    )
  )
})

test_that("schema_from_column_metadata() works for point", {
  bare_point <- geoarrow_schema_point(dim = "xy")
  bare_point$metadata <- NULL

  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point(crs = "EPSG:1234"),
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
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point(dim = "xyzm", crs = "EPSG:1234"),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for point struct", {
  bare_point <- geoarrow_schema_point_struct(dim = "xy")
  bare_point$metadata <- NULL

  schema_reconstructed <- schema_from_column_metadata(
    list(crs = NULL, encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point_struct(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point", dim = "xy"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point_struct(crs = "EPSG:1234"),
      recursive = TRUE
    )
  )

  # with dim
  bare_point$children <- lapply(1:4, function(x) narrow::narrow_schema("g"))
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point", dim = "xyzm"),
    bare_point
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_point_struct(dim = "xyzm", crs = "EPSG:1234"),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for linestring", {
  bare <- geoarrow_schema_linestring()
  bare$metadata <- NULL

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = "linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
      encoding = "linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
      encoding = "linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, geodesic = TRUE, dim = "xy",
      encoding = "linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(
        geodesic = TRUE,
        point = geoarrow_schema_point()
      ),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for polygon", {
  bare <- geoarrow_schema_polygon()
  bare$metadata <- NULL

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = "polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
      encoding = "polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(crs = "EPSG:1234")
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
      encoding = "polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(
        point = geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, geodesic = TRUE, dim = "xy",
      encoding = "polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(
        geodesic = TRUE,
        point = geoarrow_schema_point()
      ),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for multipoint", {
  bare <- geoarrow_schema_multi(geoarrow_schema_point())
  bare$metadata <- NULL
  bare$children[[1]]$metadata <- NULL
  bare$children[[1]]$children[[1]]$metadata <- NULL

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = "multipoint"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_point()
      ),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
      encoding = "multipoint"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_point(crs = "EPSG:1234")
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
      encoding = "multipoint"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_point(dim = "xyzm")
      ),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for multilinestring", {
  bare <- geoarrow_schema_multi(geoarrow_schema_linestring())
  bare$metadata <- NULL
  bare$children[[1]]$metadata <- NULL
  bare$children[[1]]$children[[1]]$metadata <- NULL

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = "multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring()
      ),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
      encoding = "multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(
          point = geoarrow_schema_point(crs = "EPSG:1234")
        )
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
      encoding = "multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(
          point = geoarrow_schema_point(dim = "xyzm")
        )
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy", geodesic = TRUE,
      encoding = "multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(geodesic = TRUE)
      ),
      recursive = TRUE
    )
  )
})

test_that("schema_from_column_metadata() works for multipolygon", {
  bare <- geoarrow_schema_multi(geoarrow_schema_polygon())
  bare$metadata <- NULL
  bare$children[[1]]$metadata <- NULL
  bare$children[[1]]$children[[1]]$metadata <- NULL

  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy",
      encoding = "multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_polygon()
      ),
      recursive = TRUE
    )
  )

  # with crs
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = "EPSG:1234", dim = "xy",
      encoding = "multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_polygon(
          point = geoarrow_schema_point(crs = "EPSG:1234")
        )
      ),
      recursive = TRUE
    )
  )

  # with dim
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xyzm",
      encoding = "multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_polygon(
          point = geoarrow_schema_point(dim = "xyzm")
        )
      ),
      recursive = TRUE
    )
  )

  # with geodesic
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL, dim = "xy", geodesic = TRUE,
      encoding = "multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_polygon(geodesic = TRUE)
      ),
      recursive = TRUE
    )
  )
})

