
#' Create low-level Arrow schemas
#'
#' These schemas are used as the basis for column types in Apache Arrow
#'
#' @param crs A length-one character representation of the CRS. The WKT2
#'   representation is recommended as the most complete way to encode this
#'   information; however, any string that can be recognized by the PROJ
#'   command-line utility (e.g., "OGC:CRS84").
#' @param ellipsoidal Use `TRUE` to assert that edges should be interpolated
#'   using the shortest ellipsoidal path (great circle on a sphere).
#' @param dim A string with one character per dimension. The string must be one
#'   of xy, xyz, xym, or xyzm.
#' @param point The point schema to use for coordinates
#' @param child The child schema to use in a single-type (multi) collection
#' @param children The child schemas for children in the union
#' @param format For list-of types, the storage format. This can be +l
#'   (32-bit integer offsets), +L (64-bit integer offsets), or +w:(fixed_size).
#'   For [geo_arrow_schema_polygon()], `format` has two elements: the first
#'   for the list of rings and the second for the list of points.
#' @param format_id The type to use for a flat identifier column.
#' @inheritParams carrow::carrow_schema
#'
#' @return A [carrow_schema()].
#' @export
#'
#' @examples
#' geo_arrow_schema_point()
#' geo_arrow_schema_linestring()
#' geo_arrow_schema_polygon()
#' geo_arrow_schema_multi(geo_arrow_schema_point())
#' geo_arrow_schema_sparse_geometrycollection()
#' geo_arrow_schema_dense_geometrycollection()
#'
geo_arrow_schema_point <- function(name = "", dim = "xy", crs = NULL, nullable = TRUE) {
  geo_arrow_schema_point_float64(name, dim, crs, nullable)
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_linestring <- function(name = "", format = "+l", nullable = TRUE, ellipsoidal = FALSE,
                                        point = geo_arrow_schema_point(nullable = FALSE)) {
  carrow::carrow_schema(
    name = name,
    format = format,
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_linestring",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(ellipsoidal = ellipsoidal)
    ),
    children = list(point)
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_polygon <- function(name = "", format = c("+l", "+l"), nullable = TRUE, ellipsoidal = FALSE,
                                     point = geo_arrow_schema_point(nullable = FALSE)) {
  carrow::carrow_schema(
    name = name,
    format = format[1],
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_polygon",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(ellipsoidal = ellipsoidal)
    ),
    children = list(
      carrow::carrow_schema(format = format[2], name = "", children = list(point))
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_multi <- function(child, name = "", format = "+l", nullable = TRUE) {
  carrow::carrow_schema(
    name = name,
    format = format,
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_multi",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize()
    ),
    children = list(child)
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_sparse_geometrycollection <- function(children = list(), name = "") {
  type_ids <- paste(names(children), collapse = ",")
  carrow::carrow_schema(
    name = name,
    format = sprintf("+us:%s", type_ids),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_collection",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize()
    ),
    children = children
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_dense_geometrycollection <- function(children = list(), name = "") {
  type_ids <- paste(names(children), collapse = ",")
  schema <- geo_arrow_schema_sparse_geometrycollection(children, name)
  schema$format <- sprintf("+us:%s", type_ids)
  schema
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_wkb <- function(name = "", format = "z", crs = NULL, ellipsoidal = FALSE,
                                 nullable = TRUE) {
  stopifnot(isTRUE(format %in% c("z", "Z")))
  carrow::carrow_schema(
    name = name,
    format = format,
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_wkb",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(crs = crs, ellipsoidal = ellipsoidal)
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_wkt <- function(name = "", format = "u", crs = NULL, ellipsoidal = FALSE, nullable = TRUE) {
  stopifnot(isTRUE(format %in% c("u", "U")))
  carrow::carrow_schema(
    name = name,
    format = format,
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_wkt",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(crs = crs, ellipsoidal = ellipsoidal)
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_geojson <- function(name = "", format = "u", crs = NULL, nullable = TRUE) {
  schema <- geo_arrow_schema_wkt(name, format, crs, NULL, nullable)
  schema$metadata[["ARROW:extension:name"]] <- "geo_arrow_geojson"
  schema
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_flat_linestring <- function(name = "", format_id = "i",
                                             ellipsoidal = FALSE, nullable = TRUE,
                                             point = geo_arrow_schema_point(nullable = FALSE)) {
  carrow::carrow_schema(
    format = "+s",
    name = name,
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_flat_linestring",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(ellipsoidal = ellipsoidal)
    ),
    children = list(
      carrow::carrow_schema(
        format = format_id,
        name = "linestring_id",
        flags = carrow::carrow_schema_flags(nullable = nullable)
      ),
      point
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_flat_polygon <- function(name = "", format_id = c("i", "i"),
                                          ellipsoidal = FALSE, nullable = TRUE,
                                          point = geo_arrow_schema_point(nullable = FALSE)) {
  carrow::carrow_schema(
    format = "+s",
    name = name,
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_flat_polygon",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(ellipsoidal = ellipsoidal)
    ),
    children = list(
      carrow::carrow_schema(
        format = format_id[1],
        name = "polygon_id",
        flags = carrow::carrow_schema_flags(nullable = nullable)
      ),
      carrow::carrow_schema(
        format = format_id[2],
        name = "ring_id",
        flags = carrow::carrow_schema_flags(nullable = nullable)
      ),
      point
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_flat_geometrycollection <- function(child, name = "", format_id = "i", nullable = TRUE) {
  carrow::carrow_schema(
    format = "+s",
    name = name,
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_flat_collection",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize()
    ),
    children = list(
      carrow::carrow_schema(
        format = format_id,
        name = "collection_id",
        flags = carrow::carrow_schema_flags(nullable = nullable)
      ),
      child
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_point_struct_float64 <- function(name = "", dim = "xy", crs = NULL,
                                                  nullable = TRUE) {
  dim <- scalar_chr(dim)

  children <- lapply(strsplit(dim, ""), function(name) {
    carrow::carrow_schema("g", name = name)
  })

  carrow::carrow_schema(
    name = name,
    format = "+s",
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_point",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(crs = crs, dim = dim)
    ),
    children = children
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_point_struct_float32 <- function(name = NULL, dim = "xy", crs = NULL,
                                                  nullable = TRUE) {
  schema <- geo_arrow_schema_point_struct_float64(name, dim, crs, nullable)
  schema$children <- lapply(schema$children, function(s) {
    s$format = "f"
  })
  schema
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_point_float64 <- function(name = NULL, dim = "xy", crs = NULL,
                                           nullable = TRUE) {
  dim <- scalar_chr(dim)
  n_dim <- nchar(dim)

  carrow::carrow_schema(
    name = name,
    format = sprintf("+w:%d", n_dim),
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_point",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(crs = crs, dim = dim)
    ),
    children = list(
      carrow::carrow_schema(format = "g", name = "")
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_point_float32 <- function(name = NULL, dim = "xy", crs = NULL,
                                           nullable = TRUE) {
  schema <- geo_arrow_schema_point_float64(name, dim, crs, nullable)
  schema$children[[1]]$format <- "f"
  schema
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_point_s2 <- function(name = "", crs = "OGC:CRS84", nullable = TRUE) {
  carrow::carrow_schema(
    name = name,
    format = "L",
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_point_s2",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(crs = crs)
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_point_h3 <- function(name = "", crs = "OGC:CRS84", nullable = TRUE) {
  carrow::carrow_schema(
    name = name,
    format = "L",
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_point_h3",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize(crs = crs)
    )
  )
}
