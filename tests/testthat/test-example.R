
test_that("all examples can be created with default arguments", {
  for (which in names(geoarrow::geoarrow_example_wkt)) {
    expect_s3_class(
      geoarrow_example(!! which),
      "narrow_vctr_geoarrow"
    )
  }
})

test_that("all examples can be created into arrow::Array", {
  skip_if_not_installed("arrow")

  for (which in names(geoarrow::geoarrow_example_wkt)) {
    expect_s3_class(
      geoarrow_example_Array(!! which),
      "Array"
    )
  }
})

test_that("requested example schema is respected", {
  expect_identical(
    geoarrow_example_narrow_array()$schema$metadata[["ARROW:extension:name"]],
    "geoarrow.multipolygon"
  )
  expect_identical(
    geoarrow_example_narrow_array(schema = geoarrow_schema_wkb())$schema$format,
    "z"
  )
  expect_identical(
    geoarrow_example_narrow_array(schema = geoarrow_schema_wkt())$schema$format,
    "u"
  )
})

test_that("requested example crs is respected", {
  expect_identical(
    wk::wk_crs(geoarrow_example(crs = "EPSG:1234")),
    "EPSG:1234"
  )
})

test_that("requested example edges field is respected", {
  array_spherical <- geoarrow_example_narrow_array(edges = "spherical")
  expect_identical(
    geoarrow_metadata(array_spherical$schema$children[[1]])$edges,
    "spherical"
  )
  expect_true(wk::wk_is_geodesic(array_spherical))
})
