
#' Write geometry as Apache Parquet files
#'
#' @inheritDotParams arrow::write_parquet
#' @inheritParams geoarrow_create
#'
#' @return The result of [arrow::write_parquet()], invisibly
#' @export
#'
write_geoarrow_parquet <- function(handleable, ..., schema = NULL, strict = FALSE) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for write_geoarrow_parquet()", call. = FALSE) # nocov
  }

  if (!is.data.frame(handleable)) {
    handleable <- data.frame(geometry = handleable)
  } else {
    handleable <- as.data.frame(handleable)
  }

  is_handleable <- vapply(handleable, is_handleable_column, logical(1))
  df_attr <- handleable[!is_handleable]
  df_handleable <- handleable[is_handleable]

  if (is.null(schema)) {
    arrays_handleable <- lapply(df_handleable, geoarrow_create)
  } else {
    arrays_handleable <- lapply(
      df_handleable,
      geoarrow_create,
      schema = schema,
      strict = strict
    )
  }

  arrays_attr <- lapply(df_attr, arrow::Array$create)
  arrays_handleable_arrow <- lapply(arrays_handleable, carrow::from_carrow_array, arrow::Array)
  arrays <- c(arrays_attr, arrays_handleable_arrow)[names(handleable)]

  batch <- arrow::record_batch(!!! arrays)
  arrow::write_parquet(batch, ...)
}

is_handleable_column <- function(x) {
  tryCatch({wk::wk_vector_meta(x); TRUE}, error = function(e) FALSE)
}

parquet_metadata_column <- function(schema, include_crs = TRUE) {
  ext_name <- schema$metadata[["ARROW:extension:name"]]
  ext_meta <- geoarrow_metadata(schema)

  result <- switch(
    ext_name,
    "geoarrow.wkb" = list(
      crs = ext_meta$crs,
      geodesic = identical(ext_meta$geodesic, "true"),
      encoding = "WKB"
    ),
    "geoarrow.wkt" = list(
      crs = ext_meta$crs,
      geodesic = identical(ext_meta$geodesic, "true"),
      encoding = "WKT"
    ),
    "geoarrow.geojson" = list(crs = ext_meta$crs, encoding = "GeoJSON"),
    "geoarrow.point" = list(crs = ext_meta$crs, encoding = "point"),
    "geoarrow.linestring" = list(
      crs = geoarrow_metadata(schema$children[[1]])$crs,
      geodesic = identical(ext_meta$geodesic, "true"),
      encoding = list(
        name = "linestring",
        point = parquet_metadata_column(schema$children[[1]], include_crs = FALSE)
      )
    ),
    "geoarrow.polygon" = list(
      crs = geoarrow_metadata(schema$children[[1]]$children[[1]])$crs,
      geodesic = identical(ext_meta$geodesic, "true"),
      encoding = list(
        name = "polygon",
        point = parquet_metadata_column(
          schema$children[[1]]$children[[1]],
          include_crs = FALSE
        )
      )
    ),
    "geoarrow.multi" = {
      child <- parquet_metadata_column(schema$children[[1]], include_crs = TRUE)
      crs <- child$crs
      geodesic <- child$geodesic
      child$crs <- NULL
      child$geodesic <- NULL

      list(
        crs = crs,
        geodesic = geodesic,
        encoding = list(
          name = "multi",
          child = child
        )
      )
    }
  )


  # don't include geodesic unless TRUE
  if (!isTRUE(result$geodesic)) {
    result$geodesic <- NULL
  }

  # don't include crs or geodesic if not top level
  if (!include_crs) {
    result$crs <- NULL
    result$geodesic <- NULL
  }

  result
}
