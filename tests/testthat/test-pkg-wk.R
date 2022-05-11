
test_that("wk_crs() and wk_is_geodesic() works for arrays", {
  arr <- geoarrow_create_narrow(wk::wkt("POINT (1 2)", crs = "OGC:CRS84"))
  expect_identical(wk_crs(arr), "OGC:CRS84")
  expect_false(wk_is_geodesic(arr))

  arr <- geoarrow_create_narrow(wk::wkt("LINESTRING (1 2, 3 4)", geodesic = TRUE))
  expect_true(wk::wk_is_geodesic(arr))

  arr <- geoarrow_create_narrow(wk::wkt("POINT (1 2)"), schema = geoarrow_schema_wkt())
  expect_null(wk_crs(arr))

  arr <- geoarrow_create_narrow(
    wk::wkt("LINESTRING (1 2, 3 4)"),
    schema = geoarrow_schema_linestring(
      point = geoarrow_schema_point(crs = "1234")
    ),
    strict = TRUE
  )
  expect_identical(wk_crs(arr), "1234")

  arr <- geoarrow_create_narrow(wk::wkt("LINESTRING (1 2, 3 4)"))
  expect_null(wk_crs(arr))
})


test_that("wk_set_crs() and wk_set_geodesic() work for arrays", {
  arr <- geoarrow_create_narrow(wk::wkt("POINT (1 2)"))
  wk::wk_crs(arr) <- "OGC:CRS84"
  expect_identical(wk::wk_crs(arr), "OGC:CRS84")
  wk::wk_crs(arr) <- NULL
  expect_identical(wk::wk_crs(arr), NULL)

  arr <- geoarrow_create_narrow(wk::wkt("LINESTRING (1 2, 3 4)"))
  wk::wk_crs(arr) <- "OGC:CRS84"
  expect_identical(wk::wk_crs(arr), "OGC:CRS84")
  wk::wk_crs(arr) <- NULL
  expect_identical(wk::wk_crs(arr), NULL)

  arr <- geoarrow_create_narrow(wk::wkt("LINESTRING (1 2, 3 4)"))
  wk::wk_is_geodesic(arr) <- TRUE
  expect_true(wk::wk_is_geodesic(arr))
  wk::wk_is_geodesic(arr) <- FALSE
  expect_false(wk::wk_is_geodesic(arr))

  arr <- geoarrow_create_narrow(wk::wkt("MULTILINESTRING ((1 2, 3 4))"))
  wk::wk_is_geodesic(arr) <- TRUE
  expect_true(wk::wk_is_geodesic(arr))
  wk::wk_is_geodesic(arr) <- FALSE
  expect_false(wk::wk_is_geodesic(arr))
})

test_that("wk_set_crs() and wk_set_geodesic() work for vctrs", {
  vctr <- geoarrow(wk::wkt("POINT (1 2)"))
  wk::wk_crs(vctr) <- "OGC:CRS84"
  expect_identical(wk::wk_crs(vctr), "OGC:CRS84")
  wk::wk_crs(vctr) <- NULL
  expect_identical(wk::wk_crs(vctr), NULL)

  vctr <- geoarrow(wk::wkt("LINESTRING (1 2, 3 4)"))
  wk::wk_crs(vctr) <- "OGC:CRS84"
  expect_identical(wk::wk_crs(vctr), "OGC:CRS84")
  wk::wk_crs(vctr) <- NULL
  expect_identical(wk::wk_crs(vctr), NULL)

  vctr <- geoarrow(wk::wkt("LINESTRING (1 2, 3 4)"))
  wk::wk_is_geodesic(vctr) <- TRUE
  expect_true(wk::wk_is_geodesic(vctr))
  wk::wk_is_geodesic(vctr) <- FALSE
  expect_false(wk::wk_is_geodesic(vctr))

  vctr <- geoarrow(wk::wkt("MULTILINESTRING ((1 2, 3 4))"))
  wk::wk_is_geodesic(vctr) <- TRUE
  expect_true(wk::wk_is_geodesic(vctr))
  wk::wk_is_geodesic(vctr) <- FALSE
  expect_false(wk::wk_is_geodesic(vctr))
})
