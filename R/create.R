
#' Create GeoArrow Arrays
#'
#' @param handleable An object with a [wk::wk_handle()] method
#' @param ... Passed to [wk::wk_handle()]
#' @param schema A [narrow::narrow_schema()] to use as a storage method.
#' @param strict Use `TRUE` to respect choices of storage type, dimensions,
#'   and CRS provided by `schema`. The default, `FALSE`, updates these values
#'   to match the data.
#' @param null_point_as_empty Use `TRUE` when creating point arrays for Parquet
#'   to work around a bug whereby null items cannot be re-read by Arrow Parquet
#'   readers.
#' @inheritParams geoarrow_schema_point
#'
#' @return A [narrow::narrow_array()]
#' @export
#'
#' @examples
#' geoarrow_create_narrow(wk::xy(1:5, 1:5))
#'
geoarrow_create_narrow <- function(handleable, ..., schema = geoarrow_schema_default(handleable),
                                   strict = FALSE, null_point_as_empty = FALSE) {
  schema <- narrow::as_narrow_schema(schema)

  if (!strict) {
    crs <- wk::wk_crs(handleable)
    if (inherits(crs, "wk_crs_inherit")) {
      crs <- NULL
    }

    schema <- geoarrow_schema_set_crs(
      schema,
      wk::wk_crs_proj_definition(crs, verbose = TRUE)
    )
    schema <- geoarrow_schema_set_geodesic(schema, wk::wk_is_geodesic(handleable))
  }

  if (inherits(handleable, c("narrow_array", "Array"))) {
    handleable <- narrow::as_narrow_array(handleable)
    result <- geoarrow_compute(
      handleable,
      "cast",
      list(schema = schema, null_is_empty = null_point_as_empty, strict = strict)
    )
  } else {
    result <- wk::wk_handle(
      handleable,
      geoarrow_compute_handler(
        "cast",
        list(schema = schema, null_is_empty = null_point_as_empty, strict = strict)
      )
    )
  }

  if (!strict) {
    result$schema <- geoarrow_copy_metadata(result$schema, schema)
  }

  result
}

#' @rdname geoarrow_create_narrow
#' @export
geoarrow_schema_default <- function(handleable, point = geoarrow_schema_point()) {
  # try vector_meta (doesn't iterate along features)
  vector_meta <- wk::wk_vector_meta(handleable)
  all_types <- vector_meta$geometry_type

  has_mising_info <- is.na(vector_meta$geometry_type) ||
    (vector_meta$geometry_type == 0L) ||
    is.na(vector_meta$has_z) ||
    is.na(vector_meta$has_m)

  # fall back on the geoparquet type collector
  if (has_mising_info) {
    types_array <- wk::wk_handle(
      handleable,
      geoarrow_compute_handler("geoparquet_types", list(include_empty = FALSE))
    )
    # A vector like c("Point", "Point Z", "MultiPoint")
    unique_types <- sort(unique(narrow::from_narrow_array(types_array)))
    unique_geom_types <- sort(unique(gsub("\\s+.*?$", "", unique_types)))

    if (length(unique_geom_types) == 1) {
      vector_meta$geometry_type <-
        wk::wk_geometry_type(tolower(unique_geom_types))
    } else if (identical(unique_geom_types, c("MultiPoint", "Point"))) {
      vector_meta$geometry_type <-
        wk::wk_geometry_type("multipoint")
    } else if (identical(unique_geom_types, c("LineString", "MultiLineString"))) {
      vector_meta$geometry_type <-
        wk::wk_geometry_type("multilinestring")
    } else if (identical(unique_geom_types, c("MultiPolygon", "Polygon"))) {
      vector_meta$geometry_type <-
        wk::wk_geometry_type("multipolygon")
    } else {
      vector_meta$geometry_type <-
        wk::wk_geometry_type("geometrycollection")
    }

    vector_meta$has_z <- any(grepl("\\s+ZM?$", unique_types))
    vector_meta$has_m <- any(grepl("\\s+Z?M$", unique_types))
  }

  point_metadata <- geoarrow_metadata(point)

  if (is.null(point_metadata$crs)) {
    crs <- wk::wk_crs(handleable)
    if (!is.null(crs) && !inherits(crs, "wk_crs_inherit")) {
      point_metadata$crs <- wk::wk_crs_proj_definition(crs, verbose = TRUE)
    }
  }

  if (vector_meta$has_z && vector_meta$has_m) {
    dims_in_coords <- "xyzm"
  } else if (vector_meta$has_z) {
    dims_in_coords <- "xyz"
  } else if (vector_meta$has_m) {
    dims_in_coords <- "xym"
  } else {
    dims_in_coords <- "xy"
  }

  point$format <- sprintf("+w:%d", nchar(dims_in_coords))
  point$children[[1]]$name <- dims_in_coords
  point$metadata[["ARROW:extension:metadata"]] <-
    do.call(geoarrow_metadata_serialize, point_metadata)

  geoarrow_schema_default_base(vector_meta$geometry_type, all_types, point)
}

geoarrow_schema_default_base <- function(geometry_type, all_geometry_types, point) {
  switch(
    geometry_type,
    point,
    geoarrow_schema_linestring(point = point),
    geoarrow_schema_polygon(),
    geoarrow_schema_collection(point),
    geoarrow_schema_multilinestring(point = point),
    geoarrow_schema_multipolygon(point = point),
    # fall back to WKB for collections or mixed types
    geoarrow_schema_wkb(),
    stop(sprintf("Unsupported geometry type ID '%d'", geometry_type), call. = FALSE) # nocov
  )
}
