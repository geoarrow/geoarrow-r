
#' Infer a GeoArrow-native type from a vector
#'
#' @param x An object from which to infer a schema.
#' @param promote_multi Use `TRUE` to return a MULTI type when both normal and
#'   MULTI elements are in the same array.
#' @param coord_type Specify the coordinate type to use if returning
#' @param ... Passed to S3 methods.
#'
#' @return A [nanoarrow_schema][as_nanoarrow_schema]
#' @export
#'
#' @examples
#' infer_geoarrow_schema(wk::wkt("POINT (0 1)"))
#'
infer_geoarrow_schema <- function(x, ..., promote_multi = TRUE,
                                  coord_type = NULL) {
  UseMethod("infer_geoarrow_schema")
}

#' @export
infer_geoarrow_schema.default <- function(x, ..., promote_multi = TRUE,
                                          coord_type = NULL) {
  if (is.null(coord_type)) {
    coord_type <- enum$CoordType$SEPARATE
  }

  # try vector_meta (doesn't iterate along features)
  vector_meta <- wk::wk_vector_meta(x)

  has_mising_info <- is.na(vector_meta$geometry_type) ||
    (vector_meta$geometry_type == 0L) ||
    is.na(vector_meta$has_z) ||
    is.na(vector_meta$has_m)

  if (has_mising_info) {
    # Fall back on calculation from wk_meta(). This would be better with
    # the unique_geometry_types kernel (because it has the option to disregard
    # empties).

    meta <- wk::wk_meta(x)
    unique_types <- unique(meta$geometry_type)

    vector_meta$has_z <- any(meta$has_z, na.rm = TRUE)
    vector_meta$has_m <- any(meta$has_m, na.rm = TRUE)

    schema_from_types_and_dims(
      x,
      unique_types,
      has_z = any(meta$has_z, na.rm = TRUE),
      has_m = any(meta$has_m, na.rm = TRUE),
      promote_multi = promote_multi,
      coord_type = coord_type
    )
  } else {
    schema_from_types_and_dims(
      x,
      vector_meta$geometry_type,
      has_z = vector_meta$has_z,
      has_m = vector_meta$has_m,
      promote_multi = promote_multi,
      coord_type = coord_type
    )
  }
}

#' @export
infer_geoarrow_schema.nanoarrow_array <- function(x, ..., promote_multi = TRUE,
                                                  coord_type = NULL) {
  schema <- nanoarrow::infer_nanoarrow_schema(x)
  parsed <- geoarrow_schema_parse(schema)
  if (parsed$coord_type != enum$CoordType$UNKNOWN) {
    return(schema)
  }

  infer_geoarrow_schema.nanoarrow_array_stream(
    nanoarrow::basic_array_stream(list(x), schema = schema, validate = FALSE),
    promote_multi = promote_multi,
    coord_type = coord_type
  )
}


#' @export
infer_geoarrow_schema.nanoarrow_array_stream <- function(x, ..., promote_multi = TRUE,
                                                         coord_type = NULL) {
  schema <- x$get_schema()
  parsed <- geoarrow_schema_parse(schema)
  if (parsed$coord_type != enum$CoordType$UNKNOWN) {
    return(schema)
  }

  unique_types_array <- geoarrow_kernel_call_agg("unique_geometry_types_agg", x)
  unique_types_integer <- nanoarrow::convert_array(unique_types_array, integer())
  unique_types <- unique(unique_types_integer %% 1000L)
  unique_dims <- unique(unique_types_integer %/% 1000L + 1L)

  has_z <- any(unique_dims %in% c(2L, 4L))
  has_m <- any(unique_dims %in% c(3L, 4L))

  schema_from_types_and_dims(
    x,
    unique_types,
    has_z,
    has_m,
    promote_multi = promote_multi,
    coord_type = coord_type
  )
}

# nolint start: cyclocomp_linter.
schema_from_types_and_dims <- function(x, unique_types, has_z, has_m,
                                       promote_multi, coord_type) {
  if (is.null(coord_type)) {
    coord_type <- enum$CoordType$SEPARATE
  }

  unique_types <- sort(unique_types)

  if (length(unique_types) == 1 && (unique_types %in% 1:6)) {
    geometry_type <- unique_types
  } else if (promote_multi && identical(unique_types, c(1L, 4L))) {
    geometry_type <- 4L
  } else if (promote_multi && identical(unique_types, c(2L, 5L))) {
    geometry_type <- 5L
  } else if (promote_multi && identical(unique_types, c(3L, 6L))) {
    geometry_type <- 6L
  } else {
    return(wk_geoarrow_schema(x, na_extension_wkb))
  }

  geometry_type <- names(enum$GeometryType)[geometry_type + 1L]

  dims <- if (has_z && has_m) {
    "XYZM"
  } else if (has_z) {
    "XYZ"
  } else if (has_m) {
    "XYM"
  } else {
    "XY"
  }

  wk_geoarrow_schema(
    x,
    na_extension_geoarrow,
    geometry_type,
    dimensions = dims,
    coord_type = coord_type
  )
}
# nolint end
