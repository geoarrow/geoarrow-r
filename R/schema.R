
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
#' @param format A custom storage format
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
#'
geoarrow_schema_point <- function(name = "", dim = "xy", crs = NULL,
                                  format_coord = "g") {
  stopifnot(
    dim_is_xy_xyz_xym_or_xzm(dim),
    format_is_float_or_double(format_coord)
  )
  n_dim <- nchar(dim)

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = sprintf("+w:%d", n_dim),
    metadata = list(
      "ARROW:extension:name" = "geoarrow.point",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, dim = dim)
    ),
    children = list(
      narrow::narrow_schema(
        format = format_coord,
        name = dim
      )
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_point_struct <- function(name = "", dim = "xy", crs = NULL,
                                         format_coord = "g") {
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
    metadata = list(
      "ARROW:extension:name" = "geoarrow.point",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, dim = dim)
    ),
    children = children
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_linestring <- function(name = "", geodesic = FALSE,
                                       point = geoarrow_schema_point()) {
  point$name <- "vertices"

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = "+l",
    metadata = list(
      "ARROW:extension:name" = "geoarrow.linestring",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(geodesic = geodesic)
    ),
    children = list(point)
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_polygon <- function(name = "", geodesic = FALSE,
                                    point = geoarrow_schema_point()) {
  point$name <- "points"

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = "+l",
    metadata = list(
      "ARROW:extension:name" = "geoarrow.polygon",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(geodesic = geodesic)
    ),
    children = list(
      narrow::narrow_schema(
        format = "+l",
        name = "rings",
        children = list(point)
      )
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_multipoint <- function(child, name = "", dim = "xy",
                                       crs = NULL, format_coord = "g") {
  geoarrow_schema_multi(
    geoarrow_schema_point(
      name = "points",
      dim = dim,
      crs = crs,
      format_coord = format_coord
    ),
    name = name
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_multilinestring <- function(child, name = "", geodesic = FALSE,
                                            point = geoarrow_schema_point()) {
  geoarrow_schema_multi(
    geoarrow_schema_linestring(
      name = "linestrings",
      geodesic = geodesic,
      point = point
    ),
    name = name
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_multipolygon <- function(child, name = "", geodesic = FALSE,
                                         point = geoarrow_schema_point()) {
  geoarrow_schema_multi(
    geoarrow_schema_polygon(
      name = "polygons",
      geodesic = geodesic,
      point = point
    ),
    name = name
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_multi <- function(child, name = "") {
  child_ext <- scalar_chr(child$metadata[["ARROW:extension:name"]])
  if (identical(child_ext, "geoarrow.point")) {
    child$name <- "points"
  } else if (identical(child_ext, "geoarrow.linestring")) {
    child$name <- "linestrings"
  } else if (identical(child_ext, "geoarrow.polygon")) {
    child$name <- "polygons"
  } else {
    "parts"
  }

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = "+l",
    metadata = list(
      "ARROW:extension:name" = "geoarrow.multi",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize()
    ),
    children = list(child)
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_wkb <- function(name = "", format = "z", crs = NULL, geodesic = FALSE) {
  stopifnot(startsWith(format, "w:") || isTRUE(format %in% c("z", "Z")))

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = format,
    metadata = list(
      "ARROW:extension:name" = "geoarrow.wkb",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, geodesic = geodesic)
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_wkt <- function(name = "", format = "u", crs = NULL, geodesic = FALSE) {
  stopifnot(startsWith(format, "w:") || isTRUE(format %in% c("z", "Z", "u", "U")))

  narrow::narrow_schema(
    name = scalar_chr(name),
    format = format,
    metadata = list(
      "ARROW:extension:name" = "geoarrow.wkt",
      "ARROW:extension:metadata" = geoarrow_metadata_serialize(crs = crs, geodesic = geodesic)
    )
  )
}

#' @rdname geoarrow_schema_point
#' @export
geoarrow_schema_geojson <- function(name = "", format = "u", crs = NULL) {
  schema <- geoarrow_schema_wkt(name, format, crs, NULL)
  schema$metadata[["ARROW:extension:name"]] <- "geoarrow.geojson"
  schema
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
