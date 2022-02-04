
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
    list(crs = NULL, encoding = "point")
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_point(crs = "EPSG:1234")),
    list(crs = "EPSG:1234", encoding = "point")
  )
})

test_that("geoarrow_metadata_column() works for linestring", {
  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_linestring()),
    list(
      crs = NULL,
      encoding = "linestring"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_linestring(geodesic = TRUE)),
    list(
      crs = NULL,
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
      encoding = "linestring"
    )
  )
})

test_that("geoarrow_metadata_column() works for polygon", {
  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_polygon()),
    list(
      crs = NULL,
      encoding = "polygon"
    )
  )

  expect_mapequal(
    geoarrow_metadata_column(geoarrow_schema_polygon(geodesic = TRUE)),
    list(
      crs = NULL,
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
    list(crs = NULL, encoding = "point"),
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
    list(crs = "EPSG:1234", encoding = "point"),
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
  bare_point$format <- "+w:4"
  bare_point$children[[1]]$name <- ""
  schema_reconstructed <- schema_from_column_metadata(
    list(crs = "EPSG:1234", encoding = "point"),
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
    list(crs = NULL, encoding = "point"),
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
    list(crs = "EPSG:1234", encoding = "point"),
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
    list(crs = "EPSG:1234", encoding = "point"),
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
      crs = NULL,
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
      crs = "EPSG:1234",
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
  bare$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL,
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
      crs = NULL, geodesic = TRUE,
      encoding = "linestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_linestring(
        geodesic = TRUE,
        point = geoarrow_schema_point(dim = "xyzm")
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
      crs = NULL,
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
      crs = "EPSG:1234",
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
  bare$children[[1]]$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL,
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
      crs = NULL, geodesic = TRUE,
      encoding = "polygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_polygon(
        geodesic = TRUE,
        point = geoarrow_schema_point(dim = "xyzm")
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
      crs = NULL,
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
      crs = "EPSG:1234",
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
  bare$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL,
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
      crs = NULL,
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
      crs = "EPSG:1234",
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
  bare$children[[1]]$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL,
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
      crs = NULL, geodesic = TRUE,
      encoding = "multilinestring"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_linestring(
          geodesic = TRUE,
          point = geoarrow_schema_point(dim = "xyzm")
        )
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
      crs = NULL,
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
      crs = "EPSG:1234",
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
  bare$children[[1]]$children[[1]]$children[[1]]$children[[1]]$name <- "xyzm"
  bare$children[[1]]$children[[1]]$children[[1]]$format <- "+w:4"
  schema_reconstructed <- schema_from_column_metadata(
    list(
      crs = NULL,
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
      crs = NULL, geodesic = TRUE,
      encoding = "multipolygon"
    ),
    bare
  )
  expect_identical(
    narrow::narrow_schema_info(schema_reconstructed, recursive = TRUE),
    narrow::narrow_schema_info(
      geoarrow_schema_multi(
        geoarrow_schema_polygon(
          geodesic = TRUE,
          point = geoarrow_schema_point(dim = "xyzm")
        )
      ),
      recursive = TRUE
    )
  )
})

test_that("guess_column_encoding() works for extensioned arrays", {
  expect_identical(
    guess_column_encoding(geoarrow_schema_point()),
    "point"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_linestring()),
    "linestring"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_polygon()),
    "polygon"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_multipoint()),
    "multipoint"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_multilinestring()),
    "multilinestring"
  )

  expect_identical(
    guess_column_encoding(geoarrow_schema_multipolygon()),
    "multipolygon"
  )

  schema <- geoarrow_schema_multipoint()
  schema$children[[1]] <- geoarrow_schema_multipoint()
  expect_error(guess_column_encoding(schema), "Unsupported child encoding for multi")
})

test_that("guess_column_encoding() works for unextensioned arrays", {
  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("w:10")
    ),
    "WKB"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("z")
    ),
    "WKB"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("Z")
    ),
    "WKB"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("u")
    ),
    "WKT"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("U")
    ),
    "WKT"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("+w:2", children = list(narrow::narrow_schema("f")))
    ),
    "point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema("+w:4", children = list(narrow::narrow_schema("g")))
    ),
    "point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:02",
        children = list(narrow::narrow_schema("g", name = "xy"))
      )
    ),
    "point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:03",
        children = list(narrow::narrow_schema("g", name = "xyz"))
      )
    ),
    "point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:03",
        children = list(narrow::narrow_schema("g", name = "xym"))
      )
    ),
    "point"
  )

  expect_identical(
    guess_column_encoding(
      narrow::narrow_schema(
        "+w:04",
        children = list(narrow::narrow_schema("g", name = "xyzm"))
      )
    ),
    "point"
  )

  schema <- geoarrow_schema_point_struct(dim = "xy")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "point")

  schema <- geoarrow_schema_point_struct(dim = "xyz")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "point")

  schema <- geoarrow_schema_point_struct(dim = "xym")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "point")

  schema <- geoarrow_schema_point_struct(dim = "xyzm")
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "point")

  schema <- geoarrow_schema_linestring()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "linestring")

  schema <- geoarrow_schema_polygon()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "polygon")

  schema <- geoarrow_schema_multipoint()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "multipoint")

  schema <- geoarrow_schema_multilinestring()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "multilinestring")

  schema <- geoarrow_schema_multipolygon()
  schema$metadata <- NULL
  expect_identical(guess_column_encoding(schema), "multipolygon")

  schema <- geoarrow_schema_multipoint()
  schema$metadata <- NULL
  schema$children[[1]]$name <- "geometries"
  expect_identical(guess_column_encoding(schema), "multipoint")

  expect_error(
    guess_column_encoding(narrow::narrow_schema("i")),
    "Can't guess encoding for schema"
  )

  expect_error(
    guess_column_encoding(
      narrow::narrow_schema(
        "+l",
        children = list(narrow::narrow_schema("i", name = "not_recognized"))
      )
    ),
    "Can't guess encoding for schema"
  )
})

test_that("guess_column_dim() works", {
  expect_identical(
    guess_column_dim(geoarrow_schema_point(dim = "xyz")),
    "xyz"
  )

  bare_schema <- geoarrow_schema_point(dim = "xy")
  bare_schema$children[[1]]$name <- ""
  expect_identical(
    guess_column_dim(bare_schema),
    "xy"
  )

  bare_schema <- geoarrow_schema_point(dim = "xyzm")
  bare_schema$children[[1]]$name <- ""
  expect_identical(
    guess_column_dim(bare_schema),
    "xyzm"
  )

  expect_identical(
    guess_column_dim(geoarrow_schema_point_struct(dim = "xyz")),
    "xyz"
  )

  bare_schema <- geoarrow_schema_point_struct(dim = "xy")
  for (i in seq_along(bare_schema$children)) {
    bare_schema$children[[i]]$name <- ""
  }
  expect_identical(
    guess_column_dim(bare_schema),
    "xy"
  )

  bare_schema <- geoarrow_schema_point_struct(dim = "xyzm")
  for (i in seq_along(bare_schema$children)) {
    bare_schema$children[[i]]$name <- ""
  }
  expect_identical(
    guess_column_dim(bare_schema),
    "xyzm"
  )

  expect_identical(
    guess_column_dim(
      geoarrow_schema_linestring(
        point = geoarrow_schema_point(dim = "xyz")
      )
    ),
    "xyz"
  )

  expect_identical(guess_column_dim(narrow::narrow_schema("i")), NULL)
})

