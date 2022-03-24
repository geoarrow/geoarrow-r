
test_that("geoarrow point reader works for multipoint", {
  coords_base <- wk::xy(1:20, 21:40)

  # helpful for interactive debugging
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
    for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
      dims_exploded <- strsplit(coord_dim, "")[[1]]
      features_simple <- wk::as_wkb(wk::as_xy(coords_base, dims = dims_exploded))
      features <- wk::wk_collection(
        features_simple,
        feature_id = rep(1:4, each = 5),
        geometry_type = wk::wk_geometry_type("multipoint")
      )

      features_array <- geoarrow_create_narrow_from_buffers(
        features,
        schema = geoarrow_schema_collection(
          point_schema(dim = coord_dim),
        ),
        strict = TRUE
      )

      expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
      expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
      expect_identical(
        wk::wk_vector_meta(features_array),
        data.frame(
          geometry_type = 4L,
          size = 4,
          has_z = "z" %in% dims_exploded,
          has_m = "m" %in% dims_exploded
        )
      )
      expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
    }
  }
})

test_that("geoarrow point reader works for multilinestring", {
  coords_base <- wk::xy(1:20, 21:40)

  # helpful for interactive debugging
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
    for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
      dims_exploded <- strsplit(coord_dim, "")[[1]]
      features_simple <- wk::wk_linestring(
        wk::as_xy(coords_base, dims = dims_exploded),
        feature_id = rep(1:4, each = 5)
      )
      features <- wk::wk_collection(
        features_simple,
        feature_id = rep(1:2, each = 2),
        geometry_type = wk::wk_geometry_type("multilinestring")
      )

      features_array <- geoarrow_create_narrow_from_buffers(
        features,
        schema = geoarrow_schema_multilinestring(
          point = point_schema(dim = coord_dim)
        ),
        strict = TRUE
      )

      expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
      expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
      expect_identical(
        wk::wk_vector_meta(features_array),
        data.frame(
          geometry_type = 5L,
          size = 2,
          has_z = "z" %in% dims_exploded,
          has_m = "m" %in% dims_exploded
        )
      )
      expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
    }
  }
})

test_that("geoarrow point reader works for multipolygon", {
  poly_base <- wk::as_wkb(
    c(
      "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
      "POLYGON ((0 0, 0 -1, -1 -1, -1 0, 0 0))",
      "POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))",
      "POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))"
    )
  )

  # helpful for interactive debugging
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
    for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
      # bug in wk_polygon() precludes previous approach
      # https://github.com/paleolimbot/wk/issues/134
      dims_exploded <- strsplit(coord_dim, "")[[1]]
      features <- wk::wk_collection(
        poly_base,
        feature_id = rep(1:2, each = 2),
        geometry_type = wk::wk_geometry_type("multipolygon")
      )

      if ("z" %in% dims_exploded) {
        features <- wk::wk_set_z(features, 2)
      }

      if ("m" %in% dims_exploded) {
        features <- wk::wk_set_m(features, 3)
      }

      features_array <- geoarrow_create_narrow_from_buffers(
        features,
        schema = geoarrow_schema_multipolygon(
          point = point_schema(dim = coord_dim)
        ),
        strict = TRUE
      )

      expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
      expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
      expect_identical(
        wk::wk_vector_meta(features_array),
        data.frame(
          geometry_type = 6L,
          size = 2,
          has_z = "z" %in% dims_exploded,
          has_m = "m" %in% dims_exploded
        )
      )
      expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
    }
  }
})

test_that("geoarrow.multi* reader works for null features", {
  features <- geoarrow_create_narrow_from_buffers(wk::wkt(c(NA, "MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)))")))
  expect_identical(is.na(wk::as_wkt(features)), c(TRUE, FALSE))
})

test_that("geoarrow linestring reader errors for invalid schemas", {
  schema <- geoarrow_schema_multipoint()
  schema$children[[1]] <- narrow::narrow_schema("+l")
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "collection child has an invalid schema")

  schema <- geoarrow_schema_multipoint()
  schema$children[[1]] <- narrow::narrow_schema("+l", children = list(narrow::narrow_schema("g")))
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "must be a geoarrow.point, ")

  schema <- geoarrow_schema_multipoint()
  schema$format <- "+L"
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "collection to be a list")
})
