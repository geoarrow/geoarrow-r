
test_that("nanoarrow_schema can be created for serialized types", {
  schema_wkb <- na_extension_wkb()
  expect_identical(schema_wkb$format, "z")
  expect_identical(schema_wkb$metadata[["ARROW:extension:name"]], "geoarrow.wkb")
  expect_identical(schema_wkb$metadata[["ARROW:extension:metadata"]], "{}")

  schema_large_wkb <- na_extension_large_wkb()
  expect_identical(schema_large_wkb$format, "Z")
  expect_identical(schema_large_wkb$metadata[["ARROW:extension:name"]], "geoarrow.wkb")
  expect_identical(schema_large_wkb$metadata[["ARROW:extension:metadata"]], "{}")

  schema_wkt <- na_extension_wkt()
  expect_identical(schema_wkt$format, "u")
  expect_identical(schema_wkt$metadata[["ARROW:extension:name"]], "geoarrow.wkt")
  expect_identical(schema_wkt$metadata[["ARROW:extension:metadata"]], "{}")

  schema_large_wkt <- na_extension_large_wkt()
  expect_identical(schema_large_wkt$format, "U")
  expect_identical(schema_large_wkt$metadata[["ARROW:extension:name"]], "geoarrow.wkt")
  expect_identical(schema_large_wkt$metadata[["ARROW:extension:metadata"]], "{}")
})

test_that("nanoarrow_schema can be created for native types", {
  schema_point <- na_extension_geoarrow("POINT")
  expect_identical(schema_point$format, "+s")
  expect_identical(schema_point$metadata[["ARROW:extension:name"]], "geoarrow.point")
  expect_identical(schema_point$metadata[["ARROW:extension:metadata"]], "{}")
})

test_that("nanoarrow_schema create errors for invalid combinations of parameters", {
  expect_error(
    na_extension_geoarrow("GEOMETRY"),
    "type_id not valid"
  )
})

test_that("nanoarrow_schema can be created with metadata", {
  schema <- na_extension_wkb(crs = "{}", edges = "SPHERICAL")
  expect_identical(
    schema$metadata[["ARROW:extension:metadata"]],
    '{"crs":{},"edges":"spherical"}'
  )

  schema <- na_extension_wkb(crs = "{}", edges = "PLANAR")
  expect_identical(
    schema$metadata[["ARROW:extension:metadata"]],
    '{"crs":{}}'
  )

  schema <- na_extension_wkb(crs = NULL, edges = "PLANAR")
  expect_identical(
    schema$metadata[["ARROW:extension:metadata"]],
    '{}'
  )

  schema <- na_extension_wkb(crs = "some unknown crs", edges = "PLANAR")
  expect_identical(
    schema$metadata[["ARROW:extension:metadata"]],
    '{"crs":"some unknown crs"}'
  )

  schema <- na_extension_wkb(crs = 'unknown with quote"ing', edges = "PLANAR")
  expect_identical(
    schema$metadata[["ARROW:extension:metadata"]],
    '{"crs":"unknown with quote\\"ing"}'
  )
})

test_that("geoarrow_schema_parse() can parse a schema", {
  parsed <- geoarrow_schema_parse(na_extension_geoarrow("POINT"))
  expect_identical(parsed$id, 1L)
  expect_identical(parsed$geometry_type, enum$GeometryType$POINT)
  expect_identical(parsed$dimensions, enum$Dimensions$XY)
  expect_identical(parsed$coord_type, enum$CoordType$SEPARATE)
  expect_identical(parsed$extension_name, "geoarrow.point")
  expect_identical(parsed$crs_type, enum$CrsType$NONE)
  expect_identical(parsed$crs, "")
  expect_identical(parsed$edge_type, enum$EdgeType$PLANAR)
})

test_that("geoarrow_schema_parse() errors for invalid type input", {
  expect_error(
    geoarrow_schema_parse(nanoarrow::na_bool()),
    "Expected extension type"
  )
})

test_that("geoarrow_schema_parse() errors for invalid metadata input", {
  schema <- na_extension_wkt()
  schema$metadata[["ARROW:extension:metadata"]] <- "this is invalid JSON"
  expect_error(
    geoarrow_schema_parse(schema),
    "Expected valid GeoArrow JSON metadata"
  )
})

test_that("geoarrow_schema_parse() can parse a storage schema", {
  parsed <- geoarrow_schema_parse(nanoarrow::na_string(), "geoarrow.wkt")
  expect_identical(parsed$extension_name, "geoarrow.wkt")

  expect_error(
    geoarrow_schema_parse(nanoarrow::na_string(), NA_character_),
    "extension_name must not be NA"
  )

  expect_error(
    geoarrow_schema_parse(nanoarrow::na_string(), "not an extension name"),
    "Unrecognized GeoArrow extension name"
  )
})

test_that("schema checker works", {
  expect_true(is_geoarrow_schema(na_extension_wkt()))
  expect_false(is_geoarrow_schema(nanoarrow::na_decimal128()))
})

test_that("enum matcher works", {
  expect_identical(
    enum_value(c("GEOMETRY", "MULTIPOINT", "NOT VALID"), "GeometryType"),
    c(0L, 4L, NA_integer_)
  )

  expect_identical(
    enum_value(c(0L, 4L, 9L), "GeometryType"),
    c(0L, 4L, NA_integer_)
  )
})

test_that("enum labeller works", {
  expect_identical(
    enum_label(c("GEOMETRY", "MULTIPOINT", "NOT VALID"), "GeometryType"),
    c("GEOMETRY", "MULTIPOINT", NA_character_)
  )

  expect_identical(
    enum_label(c(0L, 4L, 9L), "GeometryType"),
    c("GEOMETRY", "MULTIPOINT", NA_character_)
  )
})

test_that("enum_scalar matcher errors for bad values", {
  expect_error(
    enum_value_scalar("NOT VALID", "GeometryType"),
    "NOT VALID is not a valid enum label or value for GeometryType"
  )

  expect_error(
    enum_value_scalar(10, "GeometryType"),
    "10 is not a valid enum label or value for GeometryType"
  )
})
