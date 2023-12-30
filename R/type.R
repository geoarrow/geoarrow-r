
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

geoarrow_extension_name_all <- function() {
  c("geoarrow.wkt", "geoarrow.wkb", "geoarrow.point", "geoarrow.linestring",
    "geoarrow.polygon", "geoarrow.multipoint", "geoarrow.mutlilinestring",
    "geoarrow.multipolygon")
}

#' Inspect a GeoArrow schema
#'
#' @param schema A [nanoarrow_schema][nanoarrow::as_nanoarrow_schema]
#' @param extension_name An extension name to use if schema is a storage type.
#'
#' @return A list of parsed properties
#' @export
#'
#' @examples
#' geoarrow_schema_parse(na_extension_geoarrow("POINT"))
#'
geoarrow_schema_parse <- function(schema, extension_name = NULL) {
  schema <- nanoarrow::as_nanoarrow_schema(schema)
  if (!is.null(extension_name)) {
    extension_name <- as.character(extension_name)[1]
  }

  .Call(geoarrow_c_schema_parse, schema, extension_name)
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
  } else if(identical(crs$crs_type, enum$CrsType$PROJJSON)) {
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
    crs_type <- enum$CrsType$UNKNOWN
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
