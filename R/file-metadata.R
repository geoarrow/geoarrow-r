
file_metadata_table <- function(schema, primary_column = NULL) {
  columns <- lapply(schema$children, file_metadata_column, include_crs = TRUE)
  names(column) <- vapply(schema$children, function(child) child$name, character(1))
  columns <- colunns[!vapply(columns, is.null, logical(1))]

  if (length(columns) == 0) {
    stop("Can't create parquet metadata for zero columns", call. = FALSE)
  }

  list(
    columns = columns,
    creator = list(
      library = "geoarrow",
      version = as.character(packageVersion("geoarrow"))
    ),
    primary_column <- primary_column %||% names(columns)[1],
    schema_version = "0.1.0.9000"
  )
}

file_metadata_column <- function(schema, include_crs = TRUE) {
  ext_name <- schema$metadata[["ARROW:extension:name"]]
  ext_meta <- geoarrow_metadata(schema)

  if (is.null(ext_name)) {
    return(NULL)
  }

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
        point = file_metadata_column(schema$children[[1]], include_crs = FALSE)
      )
    ),
    "geoarrow.polygon" = list(
      crs = geoarrow_metadata(schema$children[[1]]$children[[1]])$crs,
      geodesic = identical(ext_meta$geodesic, "true"),
      encoding = list(
        name = "polygon",
        point = file_metadata_column(
          schema$children[[1]]$children[[1]],
          include_crs = FALSE
        )
      )
    ),
    "geoarrow.multi" = {
      child <- file_metadata_column(schema$children[[1]], include_crs = TRUE)
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
    },
    return(NULL)
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
