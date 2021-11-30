
test_that("nullable and non-nullable linestring arrays can be created", {
  ls_not_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, NA, 5),
    geoarrow_schema_linestring(
      nullable = FALSE,
      point = geoarrow_schema_point_struct()
    )
  )
  expect_null(ls_not_null$array_data$buffers[[1]])
  expect_identical(as.numeric(ls_not_null$array_data$buffers[[2]]), c(0, 5, 5, 10))

  ls_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, NA, 5),
    geoarrow_schema_linestring(
      nullable = TRUE,
      point = geoarrow_schema_point_struct()
    )
  )
  expect_identical(
    as.logical(ls_null$array_data$buffers[[1]])[1:3],
    c(TRUE, FALSE, TRUE)
  )
  expect_identical(as.numeric(ls_null$array_data$buffers[[2]]), c(0, 5, 5, 10))

  ls_not_null_arrow <- carrow::from_carrow_array(ls_not_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_not_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(x = double(), y = double()),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
    )
  )

  ls_null_arrow <- carrow::from_carrow_array(ls_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
    )
  )
})

test_that("nullable and non-nullable large linestring arrays can be created", {
  ls_not_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, NA, 5),
    geoarrow_schema_linestring(
      format = "+L",
      nullable = FALSE,
      point = geoarrow_schema_point_struct()
    )
  )
  expect_null(ls_not_null$array_data$buffers[[1]])
  expect_identical(as.numeric(ls_not_null$array_data$buffers[[2]]), c(0, 5, 5, 10))

  ls_null <- geoarrow_create_linestring_array(
    wk::xy(1:10, 11:20),
    c(5, NA, 5),
    geoarrow_schema_linestring(
      format = "+L",
      nullable = TRUE,
      point = geoarrow_schema_point_struct()
    )
  )
  expect_identical(
    as.logical(ls_null$array_data$buffers[[1]])[1:3],
    c(TRUE, FALSE, TRUE)
  )
  expect_identical(as.numeric(ls_null$array_data$buffers[[2]]), c(0, 5, 5, 10))

  skip_if_not_installed("arrow")

  ls_not_null_arrow <- carrow::from_carrow_array(ls_not_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_not_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(x = double(), y = double()),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
    )
  )

  ls_null_arrow <- carrow::from_carrow_array(ls_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20))
    )
  )
})

test_that("nullable and non-nullable fixed-width linestring arrays can be created", {
  # can't create not_null if some lengths are NA
  expect_error(
    geoarrow_create_linestring_array(
      wk::xy(1:15, 11:25),
      c(5, NA, 5),
      geoarrow_schema_linestring(
        format = "+w:5",
        nullable = FALSE,
        point = geoarrow_schema_point_struct()
      )
    ),
    "all\\(is.finite\\(lengths\\)\\) is not TRUE"
  )

  ls_not_null <- geoarrow_create_linestring_array(
    wk::xy(1:15, 11:25),
    c(5, 5, 5),
    geoarrow_schema_linestring(
      format = "+w:5",
      nullable = FALSE,
      point = geoarrow_schema_point_struct()
    )
  )
  expect_null(ls_not_null$array_data$buffers[[1]])

  ls_null <- geoarrow_create_linestring_array(
    wk::xy(1:15, 11:25),
    c(5, NA, 5),
    geoarrow_schema_linestring(
      format = "+w:5",
      nullable = TRUE,
      point = geoarrow_schema_point_struct()
    )
  )
  expect_identical(
    as.logical(ls_null$array_data$buffers[[1]])[1:3],
    c(TRUE, FALSE, TRUE)
  )

  skip_if_not_installed("arrow")

  ls_not_null_arrow <- carrow::from_carrow_array(ls_not_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_not_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(x = as.numeric(6:10), y = as.numeric(16:20)),
      data.frame(x = as.numeric(11:15), y = as.numeric(21:25))
    )
  )

  ls_null_arrow <- carrow::from_carrow_array(ls_null, arrow::Array)
  expect_identical(
    lapply(as.vector(ls_null_arrow), as.data.frame),
    list(
      data.frame(x = as.numeric(1:5), y = as.numeric(11:15)),
      data.frame(),
      data.frame(x = as.numeric(11:15), y = as.numeric(21:25))
    )
  )
})

test_that("linestring arrays error for invalid schemas", {
  expect_error(
    geoarrow_create_linestring_array(wk::xy(), integer(), carrow::carrow_schema("i")),
    "geoarrow::linestring"
  )

  expect_error(
    geoarrow_create_linestring_array(
      wk::xy(),
      integer(),
      carrow::carrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow::linestring",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize()
        ))
    ),
    "Unsupported nested list storage type"
  )
})

test_that("point struct arrays can be created with and without null points", {
  coords <- wk::xy(c(1:2, NA), c(3:4, NA))

  array_not_null <- geoarrow_create_point_array(
    coords,
    geoarrow_schema_point_struct(nullable = FALSE)
  )
  expect_null(array_not_null$array_data$buffers[[1]])

  array_null <- geoarrow_create_point_array(
    coords,
    geoarrow_schema_point_struct(nullable = TRUE)
  )

  expect_identical(
    as.logical(array_null$array_data$buffers[[1]])[1:length(coords)],
    c(TRUE, TRUE, FALSE)
  )
})

test_that("point struct arrays can be created for all dimensions", {
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

test_that("point arrays can't be created from invalid schemas", {
  expect_error(
    geoarrow_create_point_array(wk::xy(), carrow::carrow_schema("i")),
    "geoarrow::point"
  )

  expect_error(
    geoarrow_create_point_array(
      wk::xy(),
      carrow::carrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow::point",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize()
        ))
    ),
    "dim"
  )

  expect_error(
    geoarrow_create_point_array(
      wk::xy(),
      carrow::carrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow::point",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize(dim = "fish")
        ))
    ),
    "xyzm"
  )

  expect_error(
    geoarrow_create_point_array(
      wk::xy(),
      carrow::carrow_schema(
        "i",
        metadata = list(
          "ARROW:extension:name" = "geoarrow::point",
          "ARROW:extension:metadata" = geoarrow_metadata_serialize(dim = "xy")
        ))
    ),
    "Unsupported point storage type"
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
