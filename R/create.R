
#' Create GeoArrow Arrays
#'
#' @param handleable An object with a [wk::wk_handle()] method
#' @param ... Passed to [wk::wk_handle()]
#' @param schema A [carrow::carrow_schema()] to use as a storage method.
#'
#' @return A [carrow::carrow_array()]
#' @export
#'
#' @examples
#' geoarrow_create(wk::xy(1:5, 1:5))
#'
geoarrow_create <- function(handleable, ..., schema = geoarrow_schema_default(handleable)) {
  UseMethod("geoarrow_create")
}

#' @rdname geoarrow_create
#' @export
geoarrow_create.default <- function(handleable, ..., schema = geoarrow_schema_default(handleable)) {

}

#' @rdname geoarrow_create
#' @export
geoarrow_schema_default <- function(handleable, point = geoarrow_schema_point(nullable = FALSE)) {
  # try vector_meta (doesn't iterate along features)
  vector_meta <- wk::wk_vector_meta(handleable)
  all_types <- vector_meta$geometry_type

  has_mising_info <- is.na(vector_meta$geometry_type) ||
    (vector_meta$geometry_type == 0L) ||
    is.na(vector_meta$size) ||
    is.na(vector_meta$has_z) ||
    is.na(vector_meta$has_m)

  # fall back on wk_meta() (doesn't iterate along coordinates)
  if (has_mising_info) {
    meta <- wk::wk_meta(handleable)
    vector_meta$size <- nrow(meta)
    vector_meta$has_z <- any(meta$has_z)
    vector_meta$has_m <- any(meta$has_m)

    all_types <- sort(unique(meta$geometry_type))
    if (length(all_types) == 1L) {
      vector_meta$geometry_type <- all_types
    } else if (identical(all_types, c(1L, 4L))) {
      vector_meta$geometry_type <- wk::wk_geometry_type("multipoint")
    } else if (identical(all_types, c(2L, 5L))) {
      vector_meta$geometry_type <- wk::wk_geometry_type("multilinestring")
    } else if (identical(all_types, c(3L, 6L))) {
      vector_meta$geometry_type <- wk::wk_geometry_type("multipolygon")
    } else {
      vector_meta$geometry_type <- wk::wk_geometry_type("geometrycollection")
    }
  }

  point_metadata <- geoarrow_metadata(point)

  if (is.null(point_metadata$crs)) {
    crs <- wk::wk_crs(handleable)
    if (!is.null(crs)) {
      point_metadata$crs <- crs2crs::crs_proj_definition(crs)
    }
  }

  if (vector_meta$has_z && vector_meta$has_m) {
    point_metadata$dim <- "xyzm"
  } else if (vector_meta$has_z) {
    point_metadata$dim <- "xyz"
  } else if (vector_meta$has_m) {
    point_metadata$dim <- "xym"
  } else {
    point_metadata$dim <- "xy"
  }

  point$metadata[["ARROW:extension:metadata"]] <-
    do.call(geoarrow_metadata_serialize, point_metadata)



  geoarrow_schema_default_base(vector_meta$geometry_type, all_types, point)
}

geoarrow_schema_default_base <- function(geometry_type, all_geometry_types, point) {
  switch(
    geometry_type,
    {
      point$flags <- bitwOr(point$flags, carrow::carrow_schema_flags(nullable = TRUE))
      point
    },
    geoarrow_schema_linestring(point = point),
    geoarrow_schema_polygon(),
    geoarrow_schema_multi(point),
    geoarrow_schema_multi(geoarrow_schema_linestring(point = point)),
    geoarrow_schema_multi(geoarrow_schema_polygon(point = point)),
    geoarrow_schema_sparse_geometrycollection(
      children = lapply(
        all_geometry_types,
        geoarrow_schema_default_base,
        all_geometry_types = NULL,
        point = point
      )
    ),
    stop(sprintf("Unsupported geometry type ID '%d'", geometry_type), call. = FALSE)
  )
}
