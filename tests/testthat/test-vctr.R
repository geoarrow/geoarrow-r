
test_that("as_geoarrow_vctr() works for basic input", {
  vctr <- as_geoarrow_vctr(c("POINT (0 1)", "POINT (1 2)"))
  expect_identical(as.integer(unclass(vctr)), 1:2)
  expect_identical(as_geoarrow_vctr(vctr), vctr)

  expect_identical(infer_nanoarrow_schema(vctr)$format, "u")
  expect_identical(as_nanoarrow_schema(vctr)$format, "u")
  expect_identical(wk::as_wkt(vctr), wk::wkt(c("POINT (0 1)", "POINT (1 2)")))
})

test_that("format() works for geoarrow_vctr", {
  vctr <- as_geoarrow_vctr(c("POINT (0.222222 1.333333)", "POINT (1 2)"))
  expect_identical(
    format(vctr),
    c("<POINT (0.222222 1.333333)>", "<POINT (1 2)>")
  )
  expect_identical(
    as.character(vctr),
    c("<POINT (0.222222 1.333333)>", "<POINT (1 2)>")
  )

  opts <- options(digits = 5, width = 30)
  on.exit(options(opts))

  expect_identical(
    format(vctr),
    c("<POINT (0.22222 1.333>", "<POINT (1 2)>")
  )
})

test_that("wk crs/edge getters are implemented for geoarrow_vctr", {
  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)")))
  expect_identical(wk::wk_crs(x), NULL)
  expect_false(wk::wk_is_geodesic(x))

  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)"), crs = "EPSG:1234"))
  expect_identical(wk::wk_crs(x), "EPSG:1234")

  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)"), geodesic = TRUE))
  expect_true(wk::wk_is_geodesic(x))
})

test_that("Errors occur for unsupported subset operations", {
  vctr <- as_geoarrow_vctr("POINT (0 1)")
  expect_error(
    vctr[5:1],
    "Can't subset nanoarrow_vctr with non-slice"
  )

  expect_error(
    vctr[1] <- "something",
    "subset assignment for nanoarrow_vctr is not supported"
  )

  expect_error(
    vctr[[1]] <- "something",
    "subset assignment for nanoarrow_vctr is not supported"
  )
})
