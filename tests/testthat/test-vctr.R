
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

test_that("wk crs/edge getters/setters are implemented for geoarrow_vctr", {
  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)")))
  expect_identical(wk::wk_crs(x), NULL)
  expect_false(wk::wk_is_geodesic(x))

  wk::wk_is_geodesic(x) <- TRUE
  expect_true(wk::wk_is_geodesic(x))
  expect_identical(wk::wk_crs(x), NULL)

  x <- as_geoarrow_vctr(wk::wkt(c("POINT (0 1)"), crs = "EPSG:1234"))
  expect_identical(wk::wk_crs(x), "EPSG:1234")
  expect_false(wk::wk_is_geodesic(x))

  wk::wk_is_geodesic(x) <- TRUE
  expect_true(wk::wk_is_geodesic(x))
  expect_identical(wk::wk_crs(x), "EPSG:1234")

  wk::wk_is_geodesic(x) <- FALSE
  expect_false(wk::wk_is_geodesic(x))

  wk::wk_crs(x) <- NULL
  expect_identical(wk::wk_crs(x), NULL)
  expect_false(wk::wk_is_geodesic(x))
})

test_that("geoarrow_vctrs can be arranged, subset, and concatenated", {
  skip_if_not_installed("arrow")

  vctr <- as_geoarrow_vctr(wk::xy(1:5, 6:10))
  expect_identical(wk::as_xy(vctr[5:1]), wk::xy(5:1, 10:6))
  expect_identical(
    wk::as_xy(vctr[c(TRUE, TRUE, FALSE, FALSE, TRUE)]),
    wk::xy(c(1, 2, 5), c(6, 7, 10))
  )

  expect_identical(
    wk::as_xy(c(vctr)),
    wk::xy(1:5, 6:10)
  )

  expect_identical(
    wk::as_xy(c(vctr, vctr)),
    wk::xy(c(1:5, 1:5), c(6:10, 6:10))
  )

  vctr_wkb <- as_geoarrow_vctr(vctr, schema = geoarrow_wkb())
  expect_identical(
    wk::as_xy(c(vctr, vctr_wkb)),
    wk::xy(c(1:5, 1:5), c(6:10, 6:10))
  )
  expect_identical(
    wk::as_xy(c(vctr_wkb, vctr)),
    wk::xy(c(1:5, 1:5), c(6:10, 6:10))
  )
})

test_that("geoarrow_vctrs error for invalid subsets", {
  vctr <- as_geoarrow_vctr(wk::xy(1:5, 6:10))

  expect_error(
    vctr[1] <- "something",
    "subset assignment for nanoarrow_vctr is not supported"
  )

  expect_error(
    vctr[[1]] <- "something",
    "subset assignment for nanoarrow_vctr is not supported"
  )
})
