
#' Extension type definitions for GeoArrow extension types
#'
#' @param crs An object representing a CRS. For maximum portability,
#'   it should implement [wk::wk_crs_projjson()].
#' @param edges One of "PLANAR" or "SPHERICAL".
#' @param geometry_type One of "POINT", "LINESTRING", "POLYGON", "MULTIPOINT",
#'   "MULTILINESTRING", "MULTIPOLYGON".
#' @param dimensions One of "XY", "XYZ", "XYM", or "XYZM"
#' @param coord_type One of "SEPARATE" or "INTERLEAVED"
#'
#' @return A [nanoarrow_schema][nanoarrow::as_nanoarrow_schema].
#' @export
#'
#' @examples
#' na_extension_wkb(crs = "OGC:CRS84")
#' na_extension_geoarrow("POINT")
#'
na_extension_wkb <- function(crs = NULL, edges = "PLANAR") {
  na_extension_geoarrow_internal(
    enum$Type$WKB,
    crs = crs,
    edges = edges
  )
}

#' @rdname na_extension_wkb
#' @export
na_extension_wkt <- function(crs = NULL, edges = "PLANAR") {
  na_extension_geoarrow_internal(
    enum$Type$WKT,
    crs = crs,
    edges = edges
  )
}

#' @rdname na_extension_wkb
#' @export
na_extension_large_wkb <- function(crs = NULL, edges = "PLANAR") {
  na_extension_geoarrow_internal(
    enum$Type$LARGE_WKB,
    crs = crs,
    edges = edges
  )
}

#' @rdname na_extension_wkb
#' @export
na_extension_large_wkt <- function(crs = NULL, edges = "PLANAR") {
  na_extension_geoarrow_internal(
    enum$Type$LARGE_WKT,
    crs = crs,
    edges = edges
  )
}

#' @rdname na_extension_wkb
#' @export
na_extension_geoarrow <- function(geometry_type, dimensions = "XY",
                                  coord_type = "SEPARATE",
                                  crs = NULL, edges = "PLANAR") {
  geometry_type <- enum_value_scalar(geometry_type, "GeometryType")
  dimensions <- enum_value_scalar(dimensions, "Dimensions")
  coord_type <- enum_value_scalar(coord_type, "CoordType")

  type_id <- .Call(geoarrow_c_make_type, geometry_type, dimensions, coord_type)
  na_extension_geoarrow_internal(type_id, crs = crs, edges = edges)
}

#' GeoArrow Types
#'
#' These functions provide GeoArrow type definitions as zero-length vectors.
#'
#' @inheritParams na_extension_wkb
#'
#' @return A [geoarrow_vctr][as_geoarrow_vctr]
#' @export
#'
#' @examples
#' geoarrow_wkb()
#' geoarrow_wkt()
#' geoarrow_point()
#'
geoarrow_wkb <- function(crs = NULL, edges = "PLANAR") {
  nanoarrow::nanoarrow_vctr(
    na_extension_wkb(
      crs = crs,
      edges = edges
    ),
    subclass = "geoarrow_vctr"
  )
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_wkt <- function(crs = NULL, edges = "PLANAR") {
  nanoarrow::nanoarrow_vctr(
    na_extension_wkt(
      crs = crs,
      edges = edges
    ),
    subclass = "geoarrow_vctr"
  )
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_large_wkb <- function(crs = NULL, edges = "PLANAR") {
  nanoarrow::nanoarrow_vctr(
    na_extension_large_wkb(
      crs = crs,
      edges = edges
    ),
    subclass = "geoarrow_vctr"
  )
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_large_wkt <- function(crs = NULL, edges = "PLANAR") {
  nanoarrow::nanoarrow_vctr(
    na_extension_large_wkt(
      crs = crs,
      edges = edges
    ),
    subclass = "geoarrow_vctr"
  )
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_native <- function(geometry_type, dimensions = "XY",
                            coord_type = "SEPARATE",
                            crs = NULL, edges = "PLANAR") {
  nanoarrow::nanoarrow_vctr(
    na_extension_geoarrow(
      geometry_type = geometry_type,
      dimensions = dimensions,
      coord_type = coord_type,
      crs = crs,
      edges = edges
    ),
    subclass = "geoarrow_vctr"
  )
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_point <- function(dimensions = "XY",
                           coord_type = "SEPARATE",
                           crs = NULL, edges = "PLANAR") {
  geoarrow_native("POINT", dimensions = dimensions, coord_type = coord_type,
                  crs = crs, edges = edges)
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_linestring <- function(dimensions = "XY",
                                coord_type = "SEPARATE",
                                crs = NULL, edges = "PLANAR") {
  geoarrow_native("LINESTRING", dimensions = dimensions, coord_type = coord_type,
                  crs = crs, edges = edges)
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_polygon <- function(dimensions = "XY",
                             coord_type = "SEPARATE",
                             crs = NULL, edges = "PLANAR") {
  geoarrow_native("POLYGON", dimensions = dimensions, coord_type = coord_type,
                  crs = crs, edges = edges)
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_multipoint <- function(dimensions = "XY",
                                coord_type = "SEPARATE",
                                crs = NULL, edges = "PLANAR") {
  geoarrow_native("MULTIPOINT", dimensions = dimensions, coord_type = coord_type,
                  crs = crs, edges = edges)
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_multilinestring <- function(dimensions = "XY",
                                     coord_type = "SEPARATE",
                                     crs = NULL, edges = "PLANAR") {
  geoarrow_native("MULTILINESTRING", dimensions = dimensions, coord_type = coord_type,
                  crs = crs, edges = edges)
}

#' @rdname geoarrow_wkb
#' @export
geoarrow_multipolygon <- function(dimensions = "XY",
                                  coord_type = "SEPARATE",
                                  crs = NULL, edges = "PLANAR") {
  geoarrow_native("MULTIPOLYGON", dimensions = dimensions, coord_type = coord_type,
                  crs = crs, edges = edges)
}

#' Inspect a GeoArrow schema
#'
#' @param schema A [nanoarrow_schema][nanoarrow::as_nanoarrow_schema]
#' @param extension_name An extension name to use if schema is a storage type.
#' @param infer_from_storage Attempt to guess an extension name if schema is not
#'   a geoarrow extension type.
#'
#' @return A list of parsed properties
#' @export
#'
#' @examples
#' geoarrow_schema_parse(na_extension_geoarrow("POINT"))
#'
geoarrow_schema_parse <- function(schema, extension_name = NULL,
                                  infer_from_storage = FALSE) {
  schema <- nanoarrow::as_nanoarrow_schema(schema)
  if (!is.null(extension_name)) {
    extension_name <- as.character(extension_name)[1]
  } else if (infer_from_storage && is.null(schema$metadata[["ARROW:extension:name"]])) {
    # Only a few storage types have unambiguous representations
    extension_name <- switch(
      schema$format,
      "z" = ,
      "Z" = "geoarrow.wkb",
      "u" = ,
      "U" = "geoarrow.wkt",
      "+s" = "geoarrow.point"
    )
  }

  .Call(geoarrow_c_schema_parse, schema, extension_name)
}

#' @rdname geoarrow_schema_parse
#' @export
is_geoarrow_schema <- function(schema) {
  schema <- nanoarrow::as_nanoarrow_schema(schema)
  ext <- schema$metadata[["ARROW:extension:name"]]
  !is.null(ext) && (ext %in% all_extension_names())
}

#' @rdname geoarrow_schema_parse
#' @export
as_geoarrow_schema <- function(schema) {
  schema <- nanoarrow::as_nanoarrow_schema(schema)

  # If this is already a geoarrow extension type, we're ready!
  if (is_geoarrow_schema(schema)) {
    return(schema)
  }

  # Otherwise, try to infer
  parsed <- geoarrow_schema_parse(schema, infer_from_storage = TRUE)
  na_extension_geoarrow_internal(parsed$id, NULL, parsed$edge_type)
}

na_extension_geoarrow_internal <- function(type_id, crs, edges) {
  metadata <- na_extension_metadata_internal(crs, edges)
  schema <- nanoarrow::nanoarrow_allocate_schema()

  .Call(
    geoarrow_c_schema_init_extension,
    schema,
    type_id
  )

  schema$metadata[["ARROW:extension:metadata"]] <- metadata
  schema
}

na_extension_metadata_internal <- function(crs, edges) {
  crs <- sanitize_crs(crs)
  edges <- enum_value_scalar(edges, "EdgeType")

  metadata <- character()

  if (identical(crs$crs_type, enum$CrsType$UNKNOWN)) {
    metadata <- sprintf('"crs":"%s"', gsub('"', '\\\\"', crs$crs))
  } else if (identical(crs$crs_type, enum$CrsType$PROJJSON)) {
    metadata <- sprintf('"crs":%s', crs$crs)
  }

  if (identical(edges, enum$EdgeType$SPHERICAL)) {
    metadata <- c(metadata, '"edges":"spherical"')
  }

  sprintf("{%s}", paste(metadata, collapse = ","))
}

sanitize_crs <- function(crs = NULL) {
  if (is.null(crs)) {
    return(list(crs_type = enum$CrsType$NONE, crs = ""))
  }

  crs_projjson <- wk::wk_crs_projjson(crs)
  if (identical(crs_projjson, NA_character_)) {
    return(list(crs_type = enum$CrsType$UNKNOWN, crs = crs))
  }

  list(crs_type = enum$CrsType$PROJJSON, crs = crs_projjson)
}

enum_value <- function(x, enum_name) {
  values <- unlist(enum[[enum_name]])
  if (is.character(x)) {
    unname(values[x])
  } else {
    as.integer(ifelse(x %in% values, x, NA_integer_))
  }
}

enum_label <- function(x, enum_name) {
  values <- unlist(enum[[enum_name]])
  if (is.character(x)) {
    as.character(ifelse(x %in% names(values), x, NA_character_))
  } else {
    ids <- match(x, values)
    names(values)[ids]
  }
}

enum_value_scalar <- function(x, enum_name) {
  x_clean <- enum_value(x, enum_name)
  if (length(x_clean) != 1 || is.na(x_clean)) {
    stop(sprintf("%s is not a valid enum label or value for %s", x, enum_name))
  }

  x_clean
}

enum <- list(
  Type = list(
    UNINITIALIZED = 0L,
    WKB = 100001L,
    LARGE_WKB = 100002L,
    WKT = 100003L,
    LARGE_WKT = 100004L
  ),
  GeometryType = list(
    GEOMETRY = 0L,
    POINT = 1L,
    LINESTRING = 2L,
    POLYGON = 3L,
    MULTIPOINT = 4L,
    MULTILINESTRING = 5L,
    MULTIPOLYGON = 6L,
    GEOMETRYCOLLECTION = 7L
  ),
  Dimensions = list(
    UNKNOWN = 0L,
    XY = 1L,
    XYZ = 2L,
    XYM = 3L,
    XYZM = 4L
  ),
  CoordType = list(
    UNKNOWN = 0L,
    SEPARATE = 1L,
    INTERLEAVED = 2L
  ),
  CrsType = list(
    NONE = 0L,
    UNKNOWN = 1L,
    PROJJSON = 2L
  ),
  EdgeType = list(
    PLANAR = 0L,
    SPHERICAL = 1L
  )
)
