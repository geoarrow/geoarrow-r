

write_geoparquet <- function(x, ...,
                             primary_geometry_column = NULL,
                             geometry_columns = NULL,
                             write_geometry_types = NULL,
                             write_bbox = NULL,
                             check_wkb = NULL,
                             schema = NULL) {
  if (is.null(schema)) {
    schema <- infer_nanoarrow_schema(x, geoarrow_lazy = TRUE)
  } else {
    schema <- as_nanoarrow_schema(schema)
  }

  geo_meta <- geoparquet_metadata_from_schema(
    schema,
    primary_geometry_column = primary_geometry_column,
    geometry_columns = geometry_columns,
    add_geometry_types = write_geometry_types
  )

  # should work with data.frame and table input
  geometry_indices <- match(names(geo_meta$columns), names(schema$children))
  table_not_geometry <- arrow::as_arrow_table(
    x[-geometry_indices],
    schema = arrow::as_schema(
      nanoarrow::na_struct(schema$children[-geometry_indices])
    )
  )
  columns_not_geometry <- table_not_geometry$columns
  names(columns_not_geometry) <- names(table_not_geometry)

  columns_geometry <- vector("list", length(geometry_indices))
  names(columns_geometry) <- names(geo_meta$columns)
  for (geo_column_i in seq_along(geometry_indices)) {
    x_i <- geometry_indices[[geo_column_i]]
    updated_column_meta_and_chunked_array <- geoparquet_encode_chunked_array(
      x[[x_i]],
      geo_meta$columns[[geo_column_i]],
      add_geometry_types = write_geometry_types,
      add_bbox = write_bbox,
      check_wkb = check_wkb
    )

    geo_meta$columns[[geo_column_i]] <- updated_column_meta_and_chunked_array[[1]]
    columns_geometry[[geo_column_i]] <- updated_column_meta_and_chunked_array[[2]]
  }

  columns_all <- c(columns_not_geometry, columns_geometry)[names(schema$children)]
  table <- arrow::Table$create(!!!columns_all)

  # We have to use auto_unbox = TRUE for now because the CRS is
  # an R representation of PROJJSON that does not roundtrip without
  # auto_unbox = TRUE and there doesn't seem to be a way to mark literal
  # json in jsonlite.
  table$metadata$geo <- jsonlite::toJSON(geo_meta, auto_unbox = TRUE)

  # Write!
  arrow::write_parquet(table, ...)
}

geoparquet_metadata_from_schema <- function(schema,
                                            primary_geometry_column,
                                            geometry_columns,
                                            add_geometry_types) {
  primary_geometry_column <- geoparquet_guess_primary_geometry_column(
    schema,
    primary_geometry_column
  )

  columns <- geoparquet_columns_from_schema(
    schema,
    geometry_columns,
    primary_geometry_column,
    add_geometry_types
  )

  list(
    version = "1.0.0",
    primary_geometry_column = primary_geometry_column,
    columns = columns
  )
}

geoparquet_encode_chunked_array <- function(chunked_array_or_vctr,
                                            spec,
                                            add_geometry_types,
                                            add_bbox,
                                            check_wkb) {
  # Only WKB is currently supported
  if (spec$encoding == "WKB") {
    item_out_vctr <- as_geoarrow_vctr(chunked_array_or_vctr, schema = na_extension_wkb())

    # Less of a problem than in Python, because we are unlikely to start out with
    # arrow-native EWKB binary which might be short-circuited into the output
    if (isTRUE(check_wkb)) {
      stop("check_wkb TRUE not implemented")
    }

  } else {
    stop(sprintf("Expected column encoding 'WKB' but got '%s'", spec$encoding))
  }

  # These can use the unique_geometry_types and box_agg kernels when implemented
  if (isTRUE(add_geometry_types)) {
    stop("add_geometry_types TRUE not supported")
  }

  if (isTRUE(add_bbox)) {
    stop("add_bbox TRUE not supported")
  }

  # Convert to ChunkedArray, except without the extension information
  item_out_chunked_array <- as_chunked_array.geoarrow_vctr(
    item_out_vctr,
    geoarrow_extension_type = FALSE
  )

  list(spec, item_out_chunked_array)
}

geoparquet_guess_primary_geometry_column <- function(schema, primary_geometry_column) {
  if (!is.null(primary_geometry_column)) {
    return(primary_geometry_column)
  }

  schema_children <- schema$children
  if ("geometry" %in% names(schema_children)) {
    return("geometry")
  }

  if ("geography" %in% names(schema_children)) {
    return("geography")
  }

  is_geoarrow <- vapply(schema_children, is_geoarrow_schema, logical(1))
  if (any(is_geoarrow)) {
    names(schema_children)[which(is_geoarrow)[1]]
  } else {
    stop("write_geoparquet() requires at least one geometry column")
  }
}

geoparquet_columns_from_schema <- function(schema,
                                           geometry_columns,
                                           primary_geometry_column,
                                           add_geometry_types) {
  schema_children <- schema$children

  if (is.null(geometry_columns)) {
    geometry_columns <- character()
    if (!is.null(primary_geometry_column)) {
      geometry_columns <- union(geometry_columns, primary_geometry_column)
    }

    is_geoarrow <- vapply(schema_children, is_geoarrow_schema, logical(1))
    geometry_columns <- union(geometry_columns, names(schema_children)[is_geoarrow])
  }

  lapply(
    schema_children[geometry_columns],
    geoparquet_column_spec_from_type,
    add_geometry_types = add_geometry_types
  )
}

geoparquet_column_spec_from_type <- function(schema, add_geometry_types) {
  spec <- list(
    encoding = "WKB",
    geometry_types = list()
  )

  if (is_geoarrow_schema(schema)) {
    parsed <- geoarrow_schema_parse(schema)

    spec$crs <- switch(
      parsed$crs_type,
      "NONE" = NULL,
      "PROJJSON" = jsonlite::fromJSON(parsed$crs, simplifyVector = FALSE),
      {
        crs_info <- sanitize_crs(parsed$crs)
        if (crs_info$crs_type == enum$CrsType$PROJJSON) {
          jsonlite::fromJSON(parsed$crs, simplifyVector = FALSE)
        } else {
          stop("Can't convert CRS to PROJJSON")
        }
      }
    )

    if (parsed$edge_type == "SPHERICAL") {
      spec$edges <- "spherical"
    }

    maybe_known_geometry_type <- parsed$geometry_type
    maybe_known_dimensions <- parsed$dimensions

    if (!identical(add_geometry_types, FALSE) &&
        maybe_known_geometry_type != "GEOMETRY" &&
        maybe_known_dimensions != "UNKNOWN") {
      # Wrap in a list so that we can use auto_unbox = TRUE
      spec$geometry_types <- as.list(
        paste0(
          geoparquet_geometry_type_label(maybe_known_geometry_type),
          geoparquet_dimension_label(maybe_known_dimensions)
        )
      )
    }
  }

  spec
}

geoparquet_geometry_type_label <- function(x) {
  switch(
    x,
    POINT = "Point",
    LINESTRING = "LineString",
    POLYGON = "Polygon",
    MULTIPOINT = "MultiPoint",
    MULTILINESTRING = "MultiLineString",
    MULTIPOLYGON = "MultiPolygon",
    NULL
  )
}

geoparquet_dimension_label <- function(x) {
  switch(
    x,
    XY = "",
    XYZ = " Z",
    XYM = " M",
    XYZM = " ZM",
    NULL
  )
}

has_geoparquet_dependencies <- function() {
  requireNamespace("arrow", quietly = TRUE) &&
    requireNamespace("jsonlite", quietly = TRUE)
}

