
geoparquet_metadata <- function(schema, primary_column = NULL) {
  columns <- lapply(schema$children, geoparquet_column_metadata, include_crs = TRUE)
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

geoparquet_column_metadata <- function(schema, include_crs = TRUE) {
  ext_name <- schema$metadata[["ARROW:extension:name"]]
  ext_meta <- geoarrow_metadata(schema)

  if (is.null(ext_name)) {
    return(NULL)
  }

  result <- switch(
    ext_name,
    "geoarrow.wkb" = list(
      crs = ext_meta$crs,
      edges = ext_meta$edges,
      encoding = "WKB"
    ),
    "geoarrow.wkt" = list(
      crs = ext_meta$crs,
      edges = ext_meta$edges,
      encoding = "WKT"
    ),
    "geoarrow.point" = list(
      crs = ext_meta$crs,
      encoding = "geoarrow.point"
    ),
    "geoarrow.linestring" = list(
      crs = geoarrow_metadata(schema$children[[1]])$crs,
      edges = ext_meta$edges,
      encoding = "geoarrow.linestring"
    ),
    "geoarrow.polygon" = list(
      crs = geoarrow_metadata(schema$children[[1]]$children[[1]])$crs,
      edges = ext_meta$edges,
      encoding = "geoarrow.polygon"
    ),
    "geoarrow.multipoint" = ,
    "geoarrow.multilinestring" = ,
    "geoarrow.multipolygon" = ,
    "geoarrow.geometrycollection" = {
      child <- geoparquet_column_metadata(schema$children[[1]], include_crs = TRUE)
      crs <- child$crs
      edges <- child$edges
      child$crs <- NULL
      child$edges <- NULL

      list(
        crs = crs,
        edges = edges,
        encoding = ext_name
      )
    },
    return(NULL)
  )


  # don't include edges unless "spherical"
  if (!identical(result$edges, "spherical")) {
    result$edges <- NULL
  }

  # don't include crs, edges, or dim if not top level
  if (!include_crs) {
    result$crs <- NULL
    result$edges <- NULL
  }

  result
}

schema_from_geoparquet_metadata <- function(meta, schema, crs = crs_unspecified(), edges = NULL) {
  if (!is.null(schema$dictionary)) {
    schema$dictionary <- schema_from_geoparquet_metadata(
      meta,
      schema$dictionary,
      crs = crs,
      edges = edges
    )

    return(schema)
  }

  encoding <- scalar_chr(meta$encoding %||% guess_column_encoding(schema))
  dim <- guess_column_dim(schema)

  if (inherits(crs, "crs_unspecified") && ("crs" %in% names(meta))) {
    crs <- meta$crs
  } else if (inherits(crs, "crs_unspecified")) {
    crs <- guess_column_crs(schema)
  }

  if (is.null(edges)) {
    edges <- meta$edges
  }

  switch(
    encoding,
    "WKB" = geoarrow_schema_wkb(
      name = schema$name,
      format = schema$format,
      crs = crs_chr_or_null(crs),
      edges = edges
    ),
    "WKT" = geoarrow_schema_wkt(
      name = schema$name,
      format = schema$format,
      crs = crs_chr_or_null(crs),
      edges = edges
    ),
    "geoarrow.point" = {
      stopifnot(is.null(edges) || identical(edges, "planar"), !is.null(dim))

      if (identical(schema$format, "+s")) {
        stopifnot(identical(nchar(dim), length(schema$children)))

        format_coord <- vapply(schema$children, function(s) s$format, character(1))
        format_coord <- unique(format_coord)
        if (length(format_coord) != 1) {
          stop("Can't parse schema with multiple child formats as point struct", call. = FALSE)
        }

        geoarrow_schema_point_struct(
          name = schema$name,
          crs = crs_chr_or_null(crs),
          dim = dim,
          format_coord = format_coord
        )
      } else {
        stopifnot(!is.null(dim))
        geoarrow_schema_point(
          name = schema$name,
          dim = dim,
          crs = crs_chr_or_null(crs),
          format_coord = schema$children[[1]]$format
        )
      }
    },
    "geoarrow.linestring" = {
      stopifnot(identical(schema$format, "+l"))
      geoarrow_schema_linestring(
        name = schema$name,
        edges = edges,
        point = schema_from_geoparquet_metadata(
          list(encoding = "geoarrow.point"),
          schema$children[[1]],
          crs = crs_chr_or_null(crs)
        )
      )
    },
    "geoarrow.polygon" = {
      stopifnot(
        identical(schema$format, "+l"),
        identical(schema$children[[1]]$format, "+l")
      )
      geoarrow_schema_polygon(
        name = schema$name,
        edges = edges,
        point = schema_from_geoparquet_metadata(
          list(encoding = "geoarrow.point"),
          schema$children[[1]]$children[[1]],
          crs = crs_chr_or_null(crs)
        )
      )
    },
    "geoarrow.multipoint" = {
      stopifnot(identical(schema$format, "+l"))
      geoarrow_schema_collection(
        name = schema$name,
        child = schema_from_geoparquet_metadata(
          list(encoding = "geoarrow.point"),
          schema$children[[1]],
          crs = crs,
          edges = edges
        )
      )
    },
    "geoarrow.multilinestring" = {
      stopifnot(identical(schema$format, "+l"), !is.null(dim))
      geoarrow_schema_collection(
        name = schema$name,
        child = schema_from_geoparquet_metadata(
          list(encoding = "geoarrow.linestring"),
          schema$children[[1]],
          crs = crs,
          edges = edges
        )
      )
    },
    "geoarrow.multipolygon" = {
      stopifnot(identical(schema$format, "+l"), !is.null(dim))
      geoarrow_schema_collection(
        name = schema$name,
        child = schema_from_geoparquet_metadata(
          list(encoding = "geoarrow.polygon"),
          schema$children[[1]],
          crs = crs,
          edges = edges
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
      "geoarrow.point" = return("geoarrow.point"),
      "geoarrow.linestring" = return("geoarrow.linestring"),
      "geoarrow.polygon" = return("geoarrow.polygon"),
      "geoarrow.multipoint" = ,
      "geoarrow.multilinestring" = ,
      "geoarrow.multipolygon" = ,
      "geoarrow.geometrycollection" = {
        child_encoding <- guess_column_encoding(schema$children[[1]])
        switch(
          child_encoding,
          "geoarrow.point" = return("geoarrow.multipoint"),
          "geoarrow.linestring" = return("geoarrow.multilinestring"),
          "geoarrow.polygon" = return("geoarrow.multipolygon"),
          "geoarrow.geometry" = return("geoarrow.geometrycollection"),
          stop(sprintf("Unsupported child encoding for collection: '%s'", child_encoding))
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
      return("geoarrow.point")
    }
  }

  # based on format + child names
  child_names <- vapply(schema$children, "[[", "name", FUN.VALUE = character(1))
  if (schema$format == "+s") {
     dims <- paste(child_names, collapse = "")
     if (dims %in% c("xy", "xyz", "xym", "xyzm")) {
       return("geoarrow.point")
     }
  } else if (length(child_names) == 1) {
    switch(
      child_names,
      "xy" =,
      "xyz" =,
      "xym" =,
      "xyzm" = return("geoarrow.point"),
      "vertices" = return("geoarrow.linestring"),
      "rings" = return("geoarrow.polygon"),
      "points" = return("geoarrow.multipoint"),
      "linestrings" = return("geoarrow.multilinestring"),
      "polygons" = return("geoarrow.multipolygon"),
      "geometries" = {
        child_encoding <- guess_column_encoding(schema$children[[1]])
        switch(
          child_encoding,
          "geoarrow.point" = return("geoarrow.multipoint"),
          "geoarrow.linestring" = return("geoarrow.multilinestring"),
          "geoarrow.polygon" = return("geoarrow.multipolygon"),
          "geoarrow.geometry" = return("geoarrow.geometrycollection")
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

guess_column_dim <- function(schema) {
  if (identical(schema$format, "+s")) {
    child_formats <- vapply(schema$children, "[[", character(1), "format")
    child_names <- vapply(schema$children, "[[", character(1), "name")
    child_names_smush <- paste(child_names, collapse = "")
    has_child_types <- all(child_formats == "g") || all(child_formats == "f")
    has_child_names <- child_names_smush %in% c("xy", "xyz", "xym", "xyzm")
    if (has_child_types && has_child_names) {
      return(child_names_smush)
    }

    if (has_child_types && length(child_names) == 2) {
      return("xy")
    } else if (has_child_types && length(child_names) == 4) {
      return("xyzm")
    }
  } else if (startsWith(schema$format, "+w:") && length(schema$children) == 1) {
    has_child_type <- schema$children[[1]]$format %in% c("f", "g")
    has_child_name <- schema$children[[1]]$name %in% c("xy", "xyz", "xym", "xyzm")
    if (has_child_type && has_child_name) {
      return(schema$children[[1]]$name)
    }

    # try to use fixed width to guess
    parsed <- narrow::parse_format(schema$format)
    if (has_child_type && identical(parsed$args$n_items, 2L)) {
      return("xy")
    } else if (has_child_type && identical(parsed$args$n_items, 4L)) {
      return("xyzm")
    }
  }

  for (child in schema$children) {
    child_dim <- guess_column_dim(child)
    if (!is.null(child_dim)) {
      return(child_dim)
    }
  }

  return(NULL)
}

guess_column_crs <- function(schema) {
  ext <- schema$metadata[["ARROW:extension:name"]]
  if (identical(ext, "geoarrow.point")) {
    return(geoarrow_metadata(schema)$crs)
  }

  for (child in schema$children) {
    child_crs <- guess_column_crs(child)
    if (!inherits(child_crs, "crs_unspecified")) {
      return(child_crs)
    }
  }

  crs_unspecified()
}

crs_unspecified <- function() {
  structure(list(), class = "crs_unspecified")
}

crs_chr_or_null <- function(crs) {
  if (inherits(crs, "crs_unspecified")) NULL else crs
}
