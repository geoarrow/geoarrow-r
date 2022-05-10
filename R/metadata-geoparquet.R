
geoparquet_metadata <- function(schema, primary_column = NULL, arrays = list(NULL)) {
  if (length(schema$children) == 0) {
    stop("Can't create parquet metadata for zero columns", call. = FALSE)
  }

  columns <- Map(geoparquet_column_metadata, schema$children, arrays, include_crs = TRUE)
  names(columns) <- vapply(schema$children, function(child) child$name, character(1))
  columns <- columns[!vapply(columns, is.null, logical(1))]
  primary_column <- if (is.null(primary_column)) names(columns)[1] else primary_column

  list(
    version = "0.3.0",
    primary_column = primary_column,
    columns = columns
  )
}

geoparquet_column_metadata <- function(schema, array = NULL, include_crs = TRUE) {
  ext_name <- schema$metadata[["ARROW:extension:name"]]
  ext_meta <- geoarrow_metadata(schema)

  if (is.null(ext_name)) {
    return(NULL)
  }

  result <- switch(
    ext_name,
    "geoarrow.wkb" = list(
      crs = ext_meta$crs %||% geoparquet_crs_if_null(),
      edges = ext_meta$edges,
      encoding = "WKB"
    ),
    "geoarrow.wkt" = list(
      crs = ext_meta$crs %||% geoparquet_crs_if_null(),
      edges = ext_meta$edges,
      encoding = "WKT"
    ),
    "geoarrow.point" = list(
      crs = ext_meta$crs %||% geoparquet_crs_if_null(),
      encoding = "geoarrow.point"
    ),
    "geoarrow.linestring" = list(
      crs = geoarrow_metadata(schema$children[[1]])$crs %||%
        geoparquet_crs_if_null(),
      edges = ext_meta$edges,
      encoding = "geoarrow.linestring"
    ),
    "geoarrow.polygon" = list(
      crs = geoarrow_metadata(schema$children[[1]]$children[[1]])$crs %||%
        geoparquet_crs_if_null(),
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

  # add bbox and geometry_type if we have the information to do so
  if (include_crs && !is.null(array)) {
    result$bbox <- geoparquet_bbox(array)
    result$geometry_type <- geoparquet_geometry_type(array)
  }

  # make sure we maintain a consistent ordering
  canonical_order <- union(
    c("encoding", "crs", "bbox", "orientation", "geometry_type"),
    names(result)
  )

  result[intersect(canonical_order, names(result))]
}

geoparquet_bbox <- function(array) {
  box <- narrow::from_narrow_array(
    geoarrow_compute(array, "global_bounds", list(null_is_empty = TRUE))
  )

  # if there is no bbox (zero length or all empties), don't return one with
  # the Inf, -Inf thing that global_bounds returns
  box_has_x <- is.finite(box$xmax - box$xmin)
  box_has_y <- is.finite(box$ymax - box$ymin)
  if (!box_has_x || !box_has_y) {
    return(NULL)
  }

  c(
    box$xmin, box$ymin,
    box$xmax, box$ymax
  )
}

geoparquet_geometry_type <- function(array) {
  # try to calculate geometry types from wk_vector_meta(),
  # which doesn't iterate along the entire array, but fall back on
  # the relatively fast 'geoparquet_types' compute function
  meta <- wk::wk_vector_meta(array)
  geom_type <- c(
    "Point", "LineString", "Polygon",
    "MultiPoint", "MultiLineString", "MultiPolygon",
    "GeometryCollection"
  )[meta$geometry_type]

  if (is.na(meta$has_z) || is.na(meta$has_m) || meta$geometry_type == 0) {
    types_array <- geoarrow_compute(
      array,
      "geoparquet_types",
      list(include_empty = FALSE)
    )
    narrow::from_narrow_array(types_array, character())
  } else if (isTRUE(meta$has_z) && isTRUE(meta$has_m)) {
    paste(geom_type, "ZM")
  } else if (isTRUE(meta$has_z)) {
    paste(geom_type, "Z")
  } else if (isTRUE(meta$has_m)) {
    paste(geom_type, "M")
  } else {
    geom_type
  }
}

schema_from_geoparquet_metadata <- function(meta, schema, crs = crs_unspecified(), edges = NULL) {
  if (!is.null(schema$dictionary)) {
    stop("dictionary-encoded encoding is not supported")
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
      geoarrow_schema_point(
        name = schema$name,
        dim = dim,
        crs = crs_chr_or_null(crs),
        format_coord = schema$children[[1]]$format
      )
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
  if (length(child_names) == 1) {
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

guess_column_dim <- function(schema, array_data = NULL) {
  if (startsWith(schema$format, "+w:") && length(schema$children) == 1) {
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
  } else if (schema$format %in% c("z", "Z", "u", "U")) {
    # for WKB and WKT, use wk_meta
    if (schema$format %in% c("z", "Z")) {
      schema$metadata[["ARROW:extension:name"]] <- "geoarrow.wkb"
    } else {
      schema$metadata[["ARROW:extension:name"]] <- "geoarrow.wkt"
    }
  }

  for (i in seq_along(schema$children)) {
    child_dim <- guess_column_dim(schema$children[[i]], array_data$children[[i]])
    if (!is.null(child_dim)) {
      return(child_dim)
    }
  }

  return(NULL)
}

guess_column_crs <- function(schema) {
  ext <- schema$metadata[["ARROW:extension:name"]]
  if (identical(ext, "geoarrow.point") ||
      identical(ext, "geoarrow.wkb") ||
      identical(ext, "geoarrow.wkt")) {
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

geoparquet_crs_if_null <- function() {
  # a literal NULL (not missing!)
  NULL
}

geoparquet_crs_if_missing <- function() {
  'GEODCRS["WGS 84 (CRS84)",
    DATUM["World Geodetic System 1984",
        ELLIPSOID["WGS 84",6378137,298.257223563,
            LENGTHUNIT["metre",1]]],
    PRIMEM["Greenwich",0,
        ANGLEUNIT["degree",0.0174532925199433]],
    CS[ellipsoidal,2],
        AXIS["geodetic longitude (Lon)",east,
            ORDER[1],
            ANGLEUNIT["degree",0.0174532925199433]],
        AXIS["geodetic latitude (Lat)",north,
            ORDER[2],
            ANGLEUNIT["degree",0.0174532925199433]],
    SCOPE["Not known."],
    AREA["World."],
    BBOX[-90,-180,90,180],
    ID["OGC","CRS84"]]'
}

crs_chr_or_null <- function(crs) {
  if (inherits(crs, "crs_unspecified")) geoparquet_crs_if_missing() else crs
}
