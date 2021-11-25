
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
#'   (32-bit integer offsets), +L (64-bit integer offsets), or +w:<fixed size>.
#'   For [geo_arrow_schema_polygon()], `format` has two elements: the first
#'   for the list of rings and the second for the list of points.
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
geo_arrow_schema_point <- function(name = NULL, dim = "xy", crs = NULL, nullable = TRUE) {
  geo_arrow_schema_point_float64(name, dim, crs, nullable)
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_linestring <- function(name = NULL, format = "+l", nullable = TRUE, ellipsoidal = FALSE,
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
geo_arrow_schema_polygon <- function(name = NULL, format = c("+l", "+l"), nullable = TRUE, ellipsoidal = FALSE,
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
      carrow::carrow_schema(format = format[2], children = list(point))
    )
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_multi <- function(child, format = "+l", name = NULL, nullable = TRUE) {
  carrow::carrow_schema(
    name = name,
    format = format,
    flags = carrow::carrow_schema_flags(nullable = nullable),
    metadata = list(
      "ARROW:extension:name" = "geo_arrow_multi",
      "ARROW:extension:metadata" = geo_arrow_metadata_serialize()
    ),
    children = list(point)
  )
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_sparse_geometrycollection <- function(children = list(), name = NULL) {
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
geo_arrow_schema_dense_geometrycollection <- function(children = list(), name = NULL) {
  type_ids <- paste(names(children), collapse = ",")
  schema <- geo_arrow_schema_sparse_geometrycollection(children, name)
  schema$format <- sprintf("+us:%s", type_ids)
  schema
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_wkb <- function(name = NULL, format = "z", crs = NULL, ellipsoidal = FALSE,
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
geo_arrow_schema_wkt <- function(name = NULL, format = "u", crs = NULL, ellipsoidal = FALSE, nullable = TRUE) {
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
geo_arrow_schema_geojson <- function(name = NULL, format = "u", crs = NULL, nullable = TRUE) {
  schema <- geo_arrow_schema_wkt(name, format, crs, NULL, nullable)
  schema$metadata[["ARROW:extension:name"]] <- "geo_arrow_geojson"
  schema
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_large_geojson <- function(name = NULL, crs = NULL, nullable = TRUE) {
  schema <- geo_arrow_schema_large_wkt(name, crs, ellipsoidal, nullable)
  schema$metadata[["ARROW:extension:name"]] <- "geo_arrow_geojson"
  schema
}

#' @rdname geo_arrow_schema_point
#' @export
geo_arrow_schema_point_struct_float64 <- function(name = NULL, dim = "xy", crs = NULL,
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
      carrow::carrow_schema(format = "g")
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
geo_arrow_schema_point_s2 <- function(name = NULL, crs = "OGC:CRS84", nullable = TRUE) {
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
geo_arrow_schema_point_h3 <- function(name = NULL, crs = "OGC:CRS84", nullable = TRUE) {
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

geo_arrow_metadata_serialize <- function(crs = NULL, ellipsoidal = NULL, dim = NULL) {
  if (!is.null(ellipsoidal)) {
    ellipsoidal <- scalar_lgl(ellipsoidal)
    ellipsoidal <- if (ellipsoidal) "true" else NULL
  }

  vals <- list(crs = crs, ellipsoidal = ellipsoidal, dim = dim)
  vals <- vals[!vapply(vals, is.null, logical(1))]
  vals <- lapply(vals, scalar_chr)

  # will be a better way!
  tmp <- tempfile()
  con <- file(tmp, open = "w+b")
  on.exit({close(con); unlink(tmp)})

  writeBin(length(vals), con, size = 4L)
  for (i in seq_along(vals)) {
    key <- charToRaw(enc2utf8(names(vals)[i]))
    value <- charToRaw(enc2utf8(vals[[i]]))

    writeBin(length(key), con, size = 4L)
    writeBin(key, con)
    writeBin(length(value), con, size = 4L)
    writeBin(value, con)
  }

  n_bytes <- seek(con)
  seek(con, 0L)
  readBin(con, what = raw(), n = n_bytes)
}

geo_arrow_metadata_deserialize <- function(metadata) {
  con <- rawConnection(metadata)
  on.exit(close(con))

  out_length <- readBin(con, integer(), size = 4L)
  out <- vector(mode = "list", length = out_length)
  out_names <- character(out_length)

  for (i in seq_along(out)) {
    key_nchar <- readBin(con, integer(), size = 4L)
    out_names[i] <- iconv(
      list(readBin(con, raw(), key_nchar)),
      from = "UTF-8", to = "UTF-8", mark = TRUE
    )[[1]]

    value_nchar <- readBin(con, integer(), size = 4L)
    out[[i]] <- iconv(
      list(readBin(con, raw(), value_nchar)),
      from = "UTF-8", to = "UTF-8", mark = TRUE
    )[[1]]
  }

  names(out) <- out_names
  out
}

temporary_to_keep_rmd_check_clean <- function() {
  .Call(geoarrow_c_schema_wkb)
}
