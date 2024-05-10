
test_that("st_as_sfc() works for geoarrow_vctr()", {
  skip_if_not_installed("sf")

  vctr <- as_geoarrow_vctr("POINT (0 1)")
  expect_identical(sf::st_as_sfc(vctr), sf::st_sfc(sf::st_point(c(0, 1))))

  vctr <- as_geoarrow_vctr(wk::wkt("POINT (0 1)"))
  expect_identical(
    sf::st_as_sfc(vctr),
    sf::st_sfc(sf::st_point(c(0, 1)))
  )
})

test_that("arrow package objects can be converted to and from sf objects", {
  skip_if_not_installed("sf")
  skip_if_not_installed("arrow")
  skip_if_not(arrow::arrow_info()$capabilities["dataset"])

  sfc <- sf::st_sfc(sf::st_point(c(0, 1)))
  sf <- sf::st_as_sf(data.frame(geometry = sfc))
  vctr <- as_geoarrow_vctr(wk::wkt("POINT (0 1)"))
  array <- arrow::as_arrow_array(vctr)
  chunked <- arrow::as_chunked_array(array)
  table <- arrow::arrow_table(geometry = chunked)
  dataset <- arrow::InMemoryDataset$create(table)
  scanner <- arrow::Scanner$create(dataset)
  reader <- arrow::as_record_batch_reader(table)

  expect_identical(sf::st_as_sfc(array), sfc)
  expect_identical(sf::st_as_sfc(chunked), sfc)
  expect_identical(
    sf::st_as_sf(table),
    sf
  )
  expect_identical(
    sf::st_as_sf(dataset),
    sf
  )
  expect_identical(
    sf::st_as_sf(scanner),
    sf
  )
  expect_identical(
    sf::st_as_sf(reader),
    sf
  )

  chunked2 <- arrow::as_chunked_array(sfc, type = arrow::as_data_type(na_extension_wkt()))
  expect_true(chunked2$Equals(chunked))

  array2 <- arrow::as_arrow_array(sfc, type = arrow::as_data_type(na_extension_wkt()))
  expect_true(array2$Equals(array))

  table2 <- arrow::as_arrow_table(
    sf,
    schema = arrow::schema(geometry = arrow::as_data_type(na_extension_wkt()))
  )
  expect_true(table2$Equals(table))

  sf_inferred <- arrow::infer_type(sfc)
  expect_identical(sf_inferred$extension_name(), "geoarrow.point")
})

test_that("sf objects work with as_geoarrow_array()", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(sf::st_point(c(0, 1)))
  sf <- sf::st_as_sf(data.frame(geometry = sfc))

  array <- as_geoarrow_array(sf)
  parsed <- geoarrow_schema_parse(infer_nanoarrow_schema(array))
  expect_identical(parsed$extension_name, "geoarrow.point")
  expect_identical(nanoarrow::convert_buffer(array$children$x$buffers[[2]]), 0)
  expect_identical(nanoarrow::convert_buffer(array$children$y$buffers[[2]]), 1)
})

test_that("convert_array() works for sfc", {
  skip_if_not_installed("sf")

  array <- as_geoarrow_array("POINT (0 1)")
  expect_identical(
    convert_array(array, sf::st_sfc()),
    sf::st_sfc(sf::st_point(c(0, 1)))
  )

  array <- as_geoarrow_array(wk::wkt("POINT (0 1)"))
  expect_identical(
    convert_array(array, sf::st_sfc()),
    sf::st_sfc(sf::st_point(c(0, 1)))
  )
})

test_that("infer_nanoarrow_schema() works for mixed sfc objects", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(),
    sf::st_linestring()
  )
  sf::st_crs(sfc) <- "OGC:CRS84"

  schema <- infer_nanoarrow_schema(sfc)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$id, enum$Type$WKB)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_nanoarrow_schema() works for single-geometry type sfc objects", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(),
    sf::st_point()
  )
  sf::st_crs(sfc) <- "OGC:CRS84"

  schema <- infer_nanoarrow_schema(sfc)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_nanoarrow_schema() works for single-geometry type sfc objects (Z)", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(1, 2, 3)),
    sf::st_point(c(1, 2, 3))
  )
  sf::st_crs(sfc) <- "OGC:CRS84"

  schema <- infer_nanoarrow_schema(sfc)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYZ)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_nanoarrow_schema() works for single-geometry type sfc objects (M)", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(1, 2, 3), dim = "XYM"),
    sf::st_point(c(1, 2, 3), dim = "XYM")
  )
  sf::st_crs(sfc) <- "OGC:CRS84"

  schema <- infer_nanoarrow_schema(sfc)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYM)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_nanoarrow_schema() works for single-geometry type sfc objects (ZM)", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(1, 2, 3, 4)),
    sf::st_point(c(1, 2, 3, 4))
  )
  sf::st_crs(sfc) <- "OGC:CRS84"

  schema <- infer_nanoarrow_schema(sfc)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYZM)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("as_nanoarrow_array() works for mixed sfc", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(NaN, NaN)),
    sf::st_linestring()
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$buffers[[3]]),
    unlist(sf::st_as_binary(sfc))
  )
})

test_that("as_nanoarrow_array() can use custom schema for sfc", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(1, 2))
  )

  array <- as_nanoarrow_array(sfc, schema = na_extension_wkb())
  expect_identical(
    as.raw(array$buffers[[3]]),
    unlist(sf::st_as_binary(sfc))
  )
})

test_that("as_nanoarrow_array() works for sfc_POINT", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(1, 2)),
    sf::st_point(c(3, 4))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 3)))
  )
  expect_identical(
    as.raw(array$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 4)))
  )
})

test_that("as_nanoarrow_array() works for sfc_POINT with mixed XYZ dimensions", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(1, 2, 3)),
    sf::st_point(c(4, 5))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 4)))
  )
  expect_identical(
    as.raw(array$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 5)))
  )
  expect_identical(
    as.raw(array$children[[3]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(3, NaN)))
  )
})

test_that("as_nanoarrow_array() works for sfc_POINT with mixed XYM dimensions", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_point(c(1, 2, 3), dim = "XYM"),
    sf::st_point(c(4, 5))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 4)))
  )
  expect_identical(
    as.raw(array$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 5)))
  )
  expect_identical(
    as.raw(array$children[[3]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(3, NaN)))
  )
})

test_that("as_nanoarrow_array() works for sfc_LINESTRING", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_linestring(rbind(c(1, 2), c(3, 4)))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 3)))
  )
  expect_identical(
    as.raw(array$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 4)))
  )
})

test_that("as_nanoarrow_array() works for sfc_LINESTRING with mixed XYZ dimensions", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_linestring(rbind(c(1, 2), c(3, 4))),
    sf::st_linestring(rbind(c(4, 5, 6), c(7, 8, 9)))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 3, 4, 7)))
  )
  expect_identical(
    as.raw(array$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 4, 5, 8)))
  )
  expect_identical(
    as.raw(array$children[[1]]$children[[3]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(NaN, NaN, 6, 9)))
  )
})

test_that("as_nanoarrow_array() works for sfc_POLYGON", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_polygon(list(rbind(c(1, 2), c(3, 4), c(5, 6), c(1, 2))))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 3, 5, 1)))
  )
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 4, 6, 2)))
  )
})

test_that("as_nanoarrow_array() works for sfc_MULTIPOINT", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_multipoint(rbind(c(1, 2), c(3, 4)))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 3)))
  )
  expect_identical(
    as.raw(array$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 4)))
  )
})

test_that("as_nanoarrow_array() works for sfc_MULTILINESTRING", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_multilinestring(list(rbind(c(1, 2), c(3, 4), c(5, 6), c(1, 2))))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 3, 5, 1)))
  )
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 4, 6, 2)))
  )
})

test_that("as_nanoarrow_array() works for sfc_MULTIPOLYGON", {
  skip_if_not_installed("sf")

  sfc <- sf::st_sfc(
    sf::st_multipolygon(list(list(rbind(c(1, 2), c(3, 4), c(5, 6), c(1, 2)))))
  )

  array <- as_nanoarrow_array(sfc)
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[1]]$children[[1]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(1, 3, 5, 1)))
  )
  expect_identical(
    as.raw(array$children[[1]]$children[[1]]$children[[1]]$children[[2]]$buffers[[2]]),
    as.raw(nanoarrow::as_nanoarrow_buffer(c(2, 4, 6, 2)))
  )
})
