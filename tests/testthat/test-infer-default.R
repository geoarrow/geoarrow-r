
test_that("infer_geoarrow_schema() works for mixed vectors", {
  vec <- wk::wkt(c("POINT (0 1)", "LINESTRING (2 3, 4 5)"), crs = "OGC:CRS84")

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$id, enum$Type$WKB)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_geoarrow_schema() works for single-type vectors", {
  vec <- wk::wkt(c("POINT (0 1)", "POINT (2 3)"), crs = "OGC:CRS84")

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$coord_type, enum$CoordType$SEPARATE)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_geoarrow_schema() respects coord_type", {
  vec <- wk::wkt(c("POINT (0 1)", "POINT (2 3)"))

  schema <- infer_geoarrow_schema(vec, coord_type = "INTERLEAVED")
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$coord_type, enum$CoordType$INTERLEAVED)
})

test_that("infer_geoarrow_schema() can promote mixed points to multi", {
  vec <- wk::wkt(c("POINT (0 1)", "MULTIPOINT (2 3)"), crs = "OGC:CRS84")

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$MULTIPOINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_geoarrow_schema() can promote mixed linestrings to multi", {
  vec <- wk::wkt(
    c("LINESTRING (0 1, 2 3)", "MULTILINESTRING ((2 3, 4 5))"),
    crs = "OGC:CRS84"
  )

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$MULTILINESTRING)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_geoarrow_schema() can promote mixed polygons to multi", {
  vec <- wk::wkt(
    c("POLYGON ((0 1, 2 3, 4 5, 0 1))", "MULTIPOLYGON (((0 1, 2 3, 4 5, 0 1)))"),
    crs = "OGC:CRS84"
  )

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$MULTIPOLYGON)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_geoarrow_schema() works for mixed dimensions (Z)", {
  vec <- wk::wkt(c("POINT (0 1)", "POINT Z (2 3 4)"))

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYZ)
})

test_that("infer_geoarrow_schema() works for mixed dimensions (M)", {
  vec <- wk::wkt(c("POINT (0 1)", "POINT M (2 3 4)"))

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYM)
})

test_that("infer_geoarrow_schema() works for mixed dimensions (ZM)", {
  vec <- wk::wkt(c("POINT Z (0 1 2)", "POINT M (2 3 4)"))

  schema <- infer_geoarrow_schema(vec)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYZM)
})

test_that("infer_geoarrow_schema() works for native arrays", {
  array <- as_geoarrow_array(wk::xy(1:5, 6:10))
  schema <- infer_geoarrow_schema(array)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$coord_type, enum$CoordType$SEPARATE)
})

test_that("infer_geoarrow_schema() works for non-native arrays", {
  array <- as_geoarrow_array(
    wk::wkt(c("POINT Z (0 1 2)", "POINT M (2 3 4)"), crs = "OGC:CRS84")
  )
  schema <- infer_geoarrow_schema(array)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYZM)
  expect_identical(parsed$coord_type, enum$CoordType$SEPARATE)
  expect_identical(parsed$crs_type, enum$CrsType$PROJJSON)
})

test_that("infer_geoarrow_schema() works for native streams", {
  array <- as_geoarrow_array(wk::xy(1:5, 6:10))
  stream <- nanoarrow::basic_array_stream(list(array))
  schema <- infer_geoarrow_schema(stream)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$coord_type, enum$CoordType$SEPARATE)
})

test_that("infer_geoarrow_schema() works for non-native streams", {
  array <- as_geoarrow_array(wk::wkt(c("POINT Z (0 1 2)", "POINT M (2 3 4)")))
  stream <- nanoarrow::basic_array_stream(list(array))
  schema <- infer_geoarrow_schema(stream)
  parsed <- geoarrow_schema_parse(schema)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XYZM)
  expect_identical(parsed$coord_type, enum$CoordType$SEPARATE)
})
