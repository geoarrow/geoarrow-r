
test_that("geoarrow point reader works for linestring", {
  coords_base <- wk::xy(1:10, 11:20)

  for (coord_container_format in c("+l", "+L", "+w:5")) {
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
          point = geoarrow_schema_point(dim = coord_dim, nullable = TRUE)
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
})
