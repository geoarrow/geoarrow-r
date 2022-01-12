
test_that("geoarrow point reader works for multipoint", {
  coords_base <- wk::xy(1:20, 21:40)

  # helpful for interactive debugging
  container_format <- "+l"
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (container_format in c("+l", "+L", "+w:5")) {
    for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
      for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
        dims_exploded <- strsplit(coord_dim, "")[[1]]
        features_simple <- wk::as_wkb(wk::as_xy(coords_base, dims = dims_exploded))
        features <- wk::wk_collection(
          features_simple,
          feature_id = rep(1:4, each = 5),
          geometry_type = wk::wk_geometry_type("multipoint")
        )

        features_array <- geoarrow_create(
          features,
          schema = geoarrow_schema_multi(
            point_schema(dim = coord_dim, nullable = TRUE),
            format = container_format
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
  }
})

test_that("geoarrow point reader works for multilinestring", {
  skip("not implemented")

  coords_base <- wk::xy(1:10, 11:20)

  # helpful for interactive debugging
  coord_container_format <- "+l"
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (coord_container_format in c("+l", "+L", "+w:5")) {
    for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
      for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
        dims_exploded <- strsplit(coord_dim, "")[[1]]
        features <- wk::wk_linestring(
          wk::as_xy(coords_base, dims = dims_exploded),
          feature_id = c(rep(1, 5), rep(2, 5))
        )

        features_array <- geoarrow_create(
          features,
          schema = geoarrow_schema_linestring(
            format = coord_container_format,
            point = point_schema(dim = coord_dim, nullable = TRUE)
          )
        )

        expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
        expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
        expect_identical(
          wk::wk_vector_meta(features_array),
          data.frame(
            geometry_type = 2L,
            size = 2,
            has_z = "z" %in% dims_exploded,
            has_m = "m" %in% dims_exploded
          )
        )
        expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
      }
    }
  }
})

test_that("geoarrow point reader works for multipolygon", {
  skip("not implemented")

  poly_base <- wk::wkt(
    c(
      "POLYGON ZM ((0 0 2 3, 1 0 2 3, 1 1 2 3, 0 1 2 3, 0 0 2 3))",
      "POLYGON ZM ((0 0 2 3, 0 -1 2 3, -1 -1 2 3, -1 0 2 3, 0 0 2 3))"
    )
  )
  coords_base <- wk::wk_vertices(poly_base)

  # helpful for interactive debugging
  poly_container_format <- "+l"
  coord_container_format <- "+l"
  point_schema <- geoarrow_schema_point
  coord_dim <- "xy"

  for (poly_container_format in c("+l", "+L", "+w:1")) {
    for (coord_container_format in c("+l", "+L", "+w:5")) {
      for (point_schema in list(geoarrow_schema_point, geoarrow_schema_point_struct)) {
        for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
          dims_exploded <- strsplit(coord_dim, "")[[1]]
          features <- wk::wk_polygon(
            wk::as_xy(coords_base, dims = dims_exploded),
            feature_id = c(rep(1, 5), rep(2, 5)),
            ring_id = c(rep(1, 5), rep(2, 5))
          )

          features_array <- geoarrow_create(
            features,
            schema = geoarrow_schema_polygon(
              format = c(poly_container_format, coord_container_format),
              point = point_schema(dim = coord_dim, nullable = TRUE)
            )
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
    }
  }
})
