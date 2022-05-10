
test_that("geoarrow point reader works for point", {
  coords_base <- wk::xy(1:10, 11:20)

  # helpful for interactive debugging
  coord_dim <- "xy"

  for (coord_dim in c("xy", "xyz", "xym", "xyzm")) {
    dims_exploded <- strsplit(coord_dim, "")[[1]]
    features <- wk::as_wkb(wk::as_xy(coords_base, dims = dims_exploded))

    features_array <- geoarrow_create_narrow_from_buffers(
      features,
      schema = geoarrow_schema_point(dim = coord_dim),
      strict = TRUE
    )

    expect_identical(wk_handle(features_array, wk::wkb_writer()), features)
    expect_identical(wk::as_wkt(features_array), wk::as_wkt(features))
    expect_identical(
      wk::wk_vector_meta(features_array),
      data.frame(
        geometry_type = 1L,
        size = 10,
        has_z = "z" %in% dims_exploded,
        has_m = "m" %in% dims_exploded
      )
    )
    expect_identical(wk::wk_meta(features_array), wk::wk_meta(features))
  }
})

test_that("geoarrow point reader errors for invalid schemas", {
  schema <- geoarrow_schema_point()
  points_array <- narrow::narrow_array(
    schema,
    narrow::narrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "array with 1 buffer")

  schema <- geoarrow_schema_point()
  schema$format <- "+w:3"
  points_array <- narrow::narrow_array(
    schema,
    narrow::narrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "'xy' to have width 2 but found width 3")

  schema <- geoarrow_schema_point()
  schema$children[[1]] <- narrow::narrow_schema("+l", name = "xy")
  points_array <- narrow::narrow_array(
    schema,
    narrow::narrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "geoarrow.point has an invalid child schema")

  schema <- geoarrow_schema_point()
  schema$children[[1]]$format <- "f"
  points_array <- narrow::narrow_array(
    schema,
    narrow::narrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "to have type Float64")

  schema <- geoarrow_schema_point()
  points_array <- narrow::narrow_array(
    schema,
    narrow::narrow_array_data(
      buffers = list(NULL)
    ),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "one child but found 0")

  schema <- geoarrow_schema_point()
  schema$format <- "+l"
  points_array <- narrow::narrow_array(
    schema,
    narrow::narrow_array_data(),
    validate = FALSE
  )
  expect_error(wk::wk_void(points_array), "a fixed-width list")
})
