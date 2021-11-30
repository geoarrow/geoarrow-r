
test_that("point struct arrays can be created", {
  coords_xy <- wk::xy(1:100, 101:200)
  array_xy <- geoarrow_create_point_array(coords_xy, geoarrow_schema_point_struct(dim = "xy"))
  expect_identical(
    carrow::from_carrow_array(array_xy),
    data.frame(x = as.double(1:100), y = as.double(101:200))
  )

  coords_xyz <- wk::xyz(1:100, 101:200, 201:300)
  array_xyz <- geoarrow_create_point_array(coords_xyz, geoarrow_schema_point_struct(dim = "xyz"))
  expect_identical(
    carrow::from_carrow_array(array_xyz),
    data.frame(x = as.double(1:100), y = as.double(101:200), z = as.double(201:300))
  )

  coords_xym <- wk::xym(1:100, 101:200, 301:400)
  array_xym <- geoarrow_create_point_array(coords_xym, geoarrow_schema_point_struct(dim = "xym"))
  expect_identical(
    carrow::from_carrow_array(array_xym),
    data.frame(x = as.double(1:100), y = as.double(101:200), m = as.double(301:400))
  )

  coords_xyzm <- wk::xyzm(1:100, 101:200, 201:300, 301:400)
  array_xyzm <- geoarrow_create_point_array(coords_xyzm, geoarrow_schema_point_struct(dim = "xyzm"))
  expect_identical(
    carrow::from_carrow_array(array_xyzm),
    data.frame(
      x = as.double(1:100), y = as.double(101:200),
      z = as.double(201:300), m = as.double(301:400)
    )
  )

  # check that these round trip to Arrow
  skip_if_not_installed("arrow")

  array_xy_arrow <- carrow::from_carrow_array(array_xy, arrow::Array)
  expect_identical(
    as.vector(array_xy_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200)
        )
      )
    )
  )

  array_xyz_arrow <- carrow::from_carrow_array(array_xyz, arrow::Array)
  expect_identical(
    as.vector(array_xyz_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200),
          z = as.numeric(201:300)
        )
      )
    )
  )

  array_xym_arrow <- carrow::from_carrow_array(array_xym, arrow::Array)
  expect_identical(
    as.vector(array_xym_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200),
          m = as.numeric(301:400)
        )
      )
    )
  )

  array_xyzm_arrow <- carrow::from_carrow_array(array_xyzm, arrow::Array)
  expect_identical(
    as.vector(array_xyzm_arrow),
    as.vector(
      arrow::Array$create(
        data.frame(
          x = as.numeric(1:100),
          y = as.numeric(101:200),
          z = as.numeric(201:300),
          m = as.numeric(301:400)
        )
      )
    )
  )
})

test_that("geoarrow_schema_default() works with and without wk::wk_vector_meta()", {
  schema_point <- geoarrow_schema_default(wk::xy(1:2, 1:2))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow::point")

  schema_point_crs <- geoarrow_schema_default(wk::xy(1:2, 1:2, crs = "EPSG:3857"))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow::point")
  geoarrow_meta <- geoarrow_metadata(schema_point_crs)
  expect_identical(geoarrow_meta$crs, "EPSG:3857")

  schema_wkt <- geoarrow_schema_default(wk::as_wkt(wk::xy(1:2, 1:2)))
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow::point")
})

test_that("geoarrow_schema_default() detects dimensions from vector_meta", {
  schema_point <- geoarrow_schema_default(wk::xy(1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point)
  expect_identical(geoarrow_meta$dim, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::xyz(1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyz)
  expect_identical(geoarrow_meta$dim, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::xym(1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xym)
  expect_identical(geoarrow_meta$dim, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::xyzm(1:2, 1:2, 1:2, 1:2))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyzm)
  expect_identical(geoarrow_meta$dim, "xyzm")
})

test_that("geoarrow_schema_default() detects dimensions from meta", {
  schema_point <- geoarrow_schema_default(wk::as_wkt(wk::xy(1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point)
  expect_identical(geoarrow_meta$dim, "xy")

  schema_point_xyz <- geoarrow_schema_default(wk::as_wkt(wk::xyz(1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyz)
  expect_identical(geoarrow_meta$dim, "xyz")

  schema_point_xym <- geoarrow_schema_default(wk::as_wkt(wk::xym(1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xym)
  expect_identical(geoarrow_meta$dim, "xym")

  schema_point_xyzm <- geoarrow_schema_default(wk::as_wkt(wk::xyzm(1:2, 1:2, 1:2, 1:2)))
  geoarrow_meta <- geoarrow_metadata(schema_point_xyzm)
  expect_identical(geoarrow_meta$dim, "xyzm")
})
