
test_that("geoarrow point reader works for polygon", {
  poly_base <- wk::wkt(
    c(
      "POLYGON ZM ((0 0 2 3, 1 0 2 3, 1 1 2 3, 0 1 2 3, 0 0 2 3))",
      "POLYGON ZM ((0 0 2 3, 0 -1 2 3, -1 -1 2 3, -1 0 2 3, 0 0 2 3))"
    )
  )
  coords_base <- wk::wk_vertices(poly_base)

  # helpful for interactive debugging
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
    for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
      dims_exploded <- strsplit(coord_dim, "")[[1]]
      features <- wk::wk_polygon(
        wk::as_xy(coords_base, dims = dims_exploded),
        feature_id = c(rep(1, 5), rep(2, 5)),
        ring_id = c(rep(1, 5), rep(2, 5))
      )

      features_array <- geoarrow_create_narrow(
        features,
        schema = geoarrow_schema_polygon(
          point = point_schema(dim = coord_dim)
        ),
        strict = TRUE
      )

      expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
      expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
      expect_identical(
        wk::wk_vector_meta(features_array),
        data.frame(
          geometry_type = 3L,
          size = 2,
          has_z = "z" %in% dims_exploded,
          has_m = "m" %in% dims_exploded
        )
      )
      expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
    }
  }
})

test_that("geoarrow polygon reader works for null features", {
  features <- geoarrow_create_narrow(wk::wkt(c(NA, "POLYGON ((0 0, 0 1, 1 0, 0 0))")))
  expect_identical(is.na(wk::as_wkt(features)), c(TRUE, FALSE))
})

test_that("geoarrow polygon reader errors for invalid schemas", {
  schema <- geoarrow_schema_polygon()
  schema$children[[1]] <- narrow::narrow_schema("+l")
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "polygon child has an invalid schema")

  schema <- geoarrow_schema_polygon()
  schema$children[[1]] <- narrow::narrow_schema("g")
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "list but found 'g'")

  schema <- geoarrow_schema_polygon()
  schema$children[[1]]$children[[1]] <- narrow::narrow_schema("+l")
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "polygon grandchild has an invalid schema")

  schema <- geoarrow_schema_polygon()
  schema$children[[1]]$children[[1]] <- narrow::narrow_schema("+l", children = list(narrow::narrow_schema("g")))
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "must be a geoarrow.point")

  schema <- geoarrow_schema_polygon()
  schema$format <- "+L"
  array <- narrow::narrow_array(schema, validate = FALSE)
  expect_error(wk::wk_void(array), "polygon to be a list")
})
