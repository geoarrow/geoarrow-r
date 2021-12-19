
file_metadata_table <- function(schema, primary_column = NULL) {
  columns <- lapply(schema$children, file_metadata_column, include_crs = TRUE)
  names(columns) <- vapply(schema$children, function(child) child$name, character(1))
  columns <- columns[!vapply(columns, is.null, logical(1))]

  if (length(columns) == 0) {
    stop("Can't create parquet metadata for zero columns", call. = FALSE)
  }

  list(
    columns = columns,
    creator = list(
      library = "geoarrow",
      version = as.character(packageVersion("geoarrow"))
    ),
    primary_column = if (is.null(primary_column)) names(columns)[1] else primary_column,
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
    "geoarrow.point" = list(
      crs = ext_meta$crs,
      dim = ext_meta$dim,
      encoding = "point"
    ),
    "geoarrow.linestring" = list(
      crs = geoarrow_metadata(schema$children[[1]])$crs,
      dim = geoarrow_metadata(schema$children[[1]])$dim,
      geodesic = identical(ext_meta$geodesic, "true"),
      encoding = list(
        name = "linestring",
        point = file_metadata_column(schema$children[[1]], include_crs = FALSE)
      )
    ),
    "geoarrow.polygon" = list(
      crs = geoarrow_metadata(schema$children[[1]]$children[[1]])$crs,
      dim = geoarrow_metadata(schema$children[[1]]$children[[1]])$dim,
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
      dim <- child$dim
      geodesic <- child$geodesic
      child$crs <- NULL
      child$geodesic <- NULL
      child$dim <- NULL

      list(
        crs = crs,
        dim = dim,
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

  # don't include dim if NULL
  if (is.null(result$dim)) {
    result$dim <- NULL
  }

  # don't include crs, geodesic, or dim if not top level
  if (!include_crs) {
    result$crs <- NULL
    result$geodesic <- NULL
    result$dim <- NULL
  }

  result
}

schema_from_column_metadata <- function(meta, schema, crs = NULL, geodesic = NULL, dim = NULL) {
  if (!is.null(schema$dictionary)) {
    schema$dictionary <- schema_from_column_metadata(
      meta,
      schema$dictionary,
      crs = crs,
      geodesic = geodesic,
      dim = dim
    )

    return(schema)
  }

  encoding <- meta$encoding
  if (is.list(meta$encoding)) {
    encoding <- encoding$name
  }

  encoding <- scalar_chr(encoding)

  if (is.null(crs)) {
    crs <- meta$crs
  }

  if (is.null(geodesic)) {
    geodesic <- meta$geodesic
  }

  if (is.null(geodesic)) {
    geodesic <- FALSE
  }

  if (is.null(dim)) {
    dim <- meta$dim
  }

  nullable <- bitwAnd(schema$flags, carrow::carrow_schema_flags(nullable = TRUE)) != 0

  switch(
    encoding,
    "WKB" = geoarrow_schema_wkb(
      name = schema$name,
      format = schema$format,
      crs = crs,
      geodesic = geodesic,
      nullable = nullable
    ),
    "WKT" = geoarrow_schema_wkt(
      name = schema$name,
      format = schema$format,
      crs = crs,
      geodesic = geodesic,
      nullable = nullable
    ),
    "GeoJSON" = geoarrow_schema_geojson(
      name = schema$name,
      format = schema$format,
      crs = crs,
      geodesic = geodesic,
      nullable = nullable
    ),
    "point" = {
      if (identical(schema$format, "+s")) {
        stopifnot(identical(nchar(dim), length(schema$children)))

        format_coord <- vapply(schema$children, function(s) s$format, character(1))
        format_coord <- unique(format_coord)
        if (length(format_coord) != 1) {
          stop("Can't parse schema with multiple child formats as point struct", call. = FALSE)
        }

        geoarrow_schema_point_struct(
          name = schema$name,
          format = schema$format,
          crs = crs,
          dim = dim,
          format_coord = format_coord,
          nullable = nullable
        )
      } else {
        geoarrow_schema_point(
          name = schema$name,
          dim = dim,
          crs = crs,
          format_coord = schema$children[[1]]$format,
          nullable = nullable
        )
      }
    },
    "linestring" = geoarrow_schema_linestring(
      name = schema$name,
      format = schema$format,
      geodesic = geodesic,
      nullable = nullable,
      point = schema_from_column_metadata(
        meta$point,
        schema$children[[1]],
        crs = crs,
        dim = dim
      )
    ),
    "polygon" = geoarrow_schema_polygon(
      name = schema$name,
      format = c(schema$format, schema$children[[1]]$format),
      geodesic = geodesic,
      nullable = nullable,
      point = schema_from_column_metadata(
        meta$point,
        schema$children[[1]]$children[[1]],
        crs = crs,
        dim = dim
      )
    ),
    "multi" = geoarrow_schema_polygon(
      name = schema$name,
      format = schema$format,
      nullable = nullable,
      child = schema_from_column_metadata(
        meta$child,
        schema$children[[1]],
        crs = crs,
        dim = dim,
        geodesic = geodesic
      )
    ),
    stop(sprintf("Unsupported encoding: '%s'", encoding))
  )
}
