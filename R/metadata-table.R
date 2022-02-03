
geoarrow_metadata_table <- function(schema, primary_column = NULL) {
  columns <- lapply(schema$children, geoarrow_metadata_column, include_crs = TRUE)
  names(columns) <- vapply(schema$children, function(child) child$name, character(1))
  columns <- columns[!vapply(columns, is.null, logical(1))]

  if (length(columns) == 0) {
    stop("Can't create parquet metadata for zero columns", call. = FALSE)
  }

  list(
    columns = columns,
    creator = list(
      library = "geoarrow",
      version = as.character(utils::packageVersion("geoarrow"))
    ),
    primary_column = if (is.null(primary_column)) names(columns)[1] else primary_column,
    schema_version = "0.1.0.9000"
  )
}

geoarrow_metadata_column <- function(schema, include_crs = TRUE) {
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
      encoding = "linestring"
    ),
    "geoarrow.polygon" = list(
      crs = geoarrow_metadata(schema$children[[1]]$children[[1]])$crs,
      dim = geoarrow_metadata(schema$children[[1]]$children[[1]])$dim,
      geodesic = identical(ext_meta$geodesic, "true"),
      encoding = "polygon"
    ),
    "geoarrow.multi" = {
      child <- geoarrow_metadata_column(schema$children[[1]], include_crs = TRUE)
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
        encoding = paste0("multi", child$encoding)
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

  encoding <- scalar_chr(meta$encoding)

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

  switch(
    encoding,
    "WKB" = geoarrow_schema_wkb(
      name = schema$name,
      format = schema$format,
      crs = crs,
      geodesic = geodesic
    ),
    "WKT" = geoarrow_schema_wkt(
      name = schema$name,
      format = schema$format,
      crs = crs,
      geodesic = geodesic
    ),
    "GeoJSON" = {
      stopifnot(identical(geodesic, FALSE))
      geoarrow_schema_geojson(
        name = schema$name,
        format = schema$format,
        crs = crs
      )
    },
    "point" = {
      stopifnot(identical(geodesic, FALSE))

      if (identical(schema$format, "+s")) {
        stopifnot(identical(nchar(dim), length(schema$children)))

        format_coord <- vapply(schema$children, function(s) s$format, character(1))
        format_coord <- unique(format_coord)
        if (length(format_coord) != 1) {
          stop("Can't parse schema with multiple child formats as point struct", call. = FALSE)
        }

        geoarrow_schema_point_struct(
          name = schema$name,
          crs = crs,
          dim = dim,
          format_coord = format_coord
        )
      } else {
        geoarrow_schema_point(
          name = schema$name,
          dim = dim,
          crs = crs,
          format_coord = schema$children[[1]]$format
        )
      }
    },
    "linestring" = {
      stopifnot(identical(schema$format, "+l"))
      geoarrow_schema_linestring(
        name = schema$name,
        geodesic = geodesic,
        point = schema_from_column_metadata(
          list(encoding = "point"),
          schema$children[[1]],
          crs = crs,
          dim = dim
        )
      )
    },
    "polygon" = {
      stopifnot(
        identical(schema$format, "+l"),
        identical(schema$children[[1]]$format, "+l")
      )
      geoarrow_schema_polygon(
        name = schema$name,
        geodesic = geodesic,
        point = schema_from_column_metadata(
          list(encoding = "point"),
          schema$children[[1]]$children[[1]],
          crs = crs,
          dim = dim
        )
      )
    },
    "multipoint" = {
      stopifnot(identical(schema$format, "+l"))
      geoarrow_schema_multi(
        name = schema$name,
        child = schema_from_column_metadata(
          list(encoding = "point"),
          schema$children[[1]],
          crs = crs,
          dim = dim,
          geodesic = geodesic
        )
      )
    },
    "multilinestring" = {
      stopifnot(identical(schema$format, "+l"))
      geoarrow_schema_multi(
        name = schema$name,
        child = schema_from_column_metadata(
          list(encoding = "linestring"),
          schema$children[[1]],
          crs = crs,
          dim = dim,
          geodesic = geodesic
        )
      )
    },
    "multipolygon" = {
      stopifnot(identical(schema$format, "+l"))
      geoarrow_schema_multi(
        name = schema$name,
        child = schema_from_column_metadata(
          list(encoding = "polygon"),
          schema$children[[1]],
          crs = crs,
          dim = dim,
          geodesic = geodesic
        )
      )
    },
    stop(sprintf("Unsupported encoding field: '%s'", encoding))
  )
}

guess_column_encoding <- function(schema) {
  schema <- narrow::as_narrow_schema(schema)

  # based on extension name:
  ext <- schema$metadata[["ARROW:extension:name"]]
  if (!is.null(ext)) {
    switch(
      ext,
      "geoarrow.wkb" = return("WKB"),
      "geoarrow.wkt" = return("WKT"),
      "geoarrow.geojson" = return("GeoJSON"),
      "geoarrow.point" = return("point"),
      "geoarrow.linestring" = return("linestring"),
      "geoarrow.polygon" = return("polygon"),
      "geoarrow.multi" = {
        child_encoding <- guess_column_encoding(schema$children[[1]])
        switch(
          child_encoding,
          "point" = return("multipoint"),
          "linestring" = return("multilinestring"),
          "polygon" = return("multipolygon"),
          stop(sprintf("Unsupported child encoding for multi: '%s'", child_encoding))
        )
      }
    )
  }

  # based on format:
  if (schema$format %in% c("Z", "z") || startsWith(schema$format, "w:")) {
    return("WKB")
  } else if (schema$format %in% c("u", "U")) {
    return("WKT")
  } else if (schema$format %in% c("+w:2", "+w:4")) {
    child_format <- schema$children[[1]]$format
    if (identical(child_format, "g") || identical(child_format, "f")) {
      return("point")
    }
  }

  # based on format + child names
  child_names <- vapply(schema$children, "[[", "name", FUN.VALUE = character(1))
  if (schema$format == "+s") {
     dims <- paste(child_names, collapse = "")
     if (dims %in% c("xy", "xyz", "xym", "xyzm")) {
       return("point")
     }
  } else if (length(child_names) == 1) {
    switch(
      child_names,
      "xy" =,
      "xyz" =,
      "xym" =,
      "xyzm" = return("point"),
      "vertices" = return("linestring"),
      "rings" = return("polygon"),
      "points" = return("multipoint"),
      "linestrings" = return("multilinestring"),
      "polygons" = return("multipolygon"),
      "geometries" = {
        child_encoding <- guess_column_encoding(schema$children[[1]])
        switch(
          child_encoding,
          "point" = return("multipoint"),
          "linestring" = return("multilinestring"),
          "polygon" = return("multipolygon")
        )
      }
    )
  }

  if (length(child_names) == 0) {
    child_names <- "<none>"
  } else {
    child_names <- paste0("'", child_names, "'", collapse = ", ")
  }

  stop(
    sprintf(
      "Can't guess encoding for schema with format '%s' and child name(s) %s",
      schema$format,
      child_names
    ),
    call. = FALSE
  )
}
