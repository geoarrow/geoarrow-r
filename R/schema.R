
#' Create low-level Arrow schemas
#'
#' These schemas are used as the basis for column types in Apache Arrow
#'
#' @param crs A length-one character representation of the CRS. The WKT2
#'   representation is recommended as the most complete way to encode this
#'   information; however, any string that can be recognized by the PROJ
#'   command-line utility (e.g., "OGC:CRS84").
#' @param geodesic Use `TRUE` to assert that edges should be interpolated
#'   using the shortest geodesic path (great circle on a sphere).
#' @param dim A string with one character per dimension. The string must be one
#'   of xy, xyz, xym, or xyzm.
#' @param point The point schema to use for coordinates
#' @param child The child schema to use in a single-type (multi) collection
#' @param children The child schemas for children in the union
#' @param format For list-of types, the storage format. This can be +l
#'   (32-bit integer offsets), +L (64-bit integer offsets), or +w:(fixed_size).
#'   For [geoarrow_schema_polygon()], `format` has two elements: the first
#'   for the list of rings and the second for the list of points.
#' @param format_coord A format for floating point coordinate storage. This
#'   can be "f" (float/float32) or "g" (double/float64).
#' @inheritParams narrow::narrow_schema
#'
#' @return A [narrow_schema()].
#' @export
#'
#' @examples
#' geoarrow_schema_point()
#' geoarrow_schema_linestring()
#' geoarrow_schema_polygon()
#' geoarrow_schema_multi(geoarrow_schema_point())
#' geoarrow_schema_sparse_geometrycollection()
#' geoarrow_schema_dense_geometrycollection()
#'
geoarrow_schema_point <- function(name = "", dim = "xy", crs = NULL,
                                  format_coord = "g", nullable = TRUE) {
  stopifnot(
    dim_is_xy_xyz_xym_or_xzm(dim),
    format_is_float_or_double(format_coord)
  )
  n_dim <- nchar(dim)

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = sprintf("+w:%d", n_dim),
    flags = narrow::narrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.point",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, dim = dim)
    ),
    children = list(
      narrow::narrow_schema(format = format_coord, name = "")
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_point_struct <- function(name = "", dim = "xy", crs = NULL,
                                         format_coord = "g", nullable = TRUE) {
  stopifnot(
    dim_is_xy_xyz_xym_or_xzm(dim),
    format_is_float_or_double(format_coord)
  )

  children <- lapply(strsplit(dim, "")[[1]], function(name) {
    narrow::narrow_schema(format_coord, name = scalar_chr(name))
  })

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = "+s",
    flags = narrow::narrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.point",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, dim = dim)
    ),
    children = children
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_linestring <- function(name = "", format = "+l", nullable = TRUE, geodesic = FALSE,
                                       point = geoarrow_schema_point()) {
  stopifnot(format_is_nested_list(format))

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = format,
    flags = narrow::narrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.linestring",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(geodesic = geodesic)
    ),
    children = list(point)
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_polygon <- function(name = "", format = c("+l", "+l"), nullable = TRUE, geodesic = FALSE,
                                     point = geoarrow_schema_point()) {
  stopifnot(
    format_is_nested_list(format[1]),
    format_is_nested_list(format[2])
  )

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = format[1],
    flags = narrow::narrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.polygon",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(geodesic = geodesic)
    ),
    children = list(
      narrow::narrow_schema(format = format[2], name = "", children = list(point))
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_multi <- function(child, name = "", format = "+l", nullable = TRUE) {
  stopifnot(format_is_nested_list(format))

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = format,
    flags = narrow::narrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.multi",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize()
    ),
    children = list(child)
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_sparse_geometrycollection <- function(children = list(), name = "") {
  type_ids <- paste(names(children), collapse = ",")
  narrow::narrow_schema(
    name = scalar_chr(name),
    format = sprintf("+us:%s", type_ids),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.collection",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize()
    ),
    children = children
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_dense_geometrycollection <- function(children = list(), name = "") {
  type_ids <- paste(names(children), collapse = ",")
  schema <- geoarrow_schema_sparse_geometrycollection(children, name)
  schema$format <- sprintf("+us:%s", type_ids)
  schema
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_wkb <- function(name = "", format = "z", crs = NULL, geodesic = FALSE,
                                 nullable = TRUE) {
  stopifnot(startsWith(format, "w:") || isTRUE(format %in% c("z", "Z")))

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = format,
    flags = narrow::narrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.wkb",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, geodesic = geodesic)
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_wkt <- function(name = "", format = "u", crs = NULL, geodesic = FALSE, nullable = TRUE) {
  stopifnot(startsWith(format, "w:") || isTRUE(format %in% c("z", "Z", "u", "U")))

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = format,
    flags = narrow::narrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.wkt",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, geodesic = geodesic)
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_geojson <- function(name = "", format = "u", crs = NULL, nullable = TRUE) {
  schema <- geoarrow_schema_wkt(name, format, crs, NULL, nullable)
  schema$metadata[["ARROW:extension:name"]] <- "geoarrow.geojson"
  schema
}

format_is_nested_list <- function(format) {
  grepl("^\\+[wlL]", scalar_chr(format))
}

format_is_id <- function(format_id) {
  isTRUE(scalar_chr(format_id) %in% c("i", "I", "l", "L", "s", "S", "c", "C"))
}

format_is_float_or_double <- function(format_coord) {
  isTRUE(scalar_chr(format_coord) %in% c("f", "g"))
}

dim_is_xy_xyz_xym_or_xzm <- function(dim) {
  grepl("^xyz?m?$", scalar_chr(dim))
}
