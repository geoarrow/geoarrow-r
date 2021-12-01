
#' Create GeoArrow Arrays
#'
#' @param handleable An object with a [wk::wk_handle()] method
#' @param ... Passed to [wk::wk_handle()]
#' @param schema A [carrow::carrow_schema()] to use as a storage method.
#' @param strict Use `TRUE` to respect choices of storage type, dimensions,
#'   and CRS provided by `schema`. The default, `FALSE`, updates these values
#'   to match the data.
#' @inheritParams geoarrow_schema_point
#'
#' @return A [carrow::carrow_array()]
#' @export
#'
#' @examples
#' geoarrow_create(wk::xy(1:5, 1:5))
#'
geoarrow_create <- function(handleable, ..., schema = geoarrow_schema_default(handleable),
                            strict = FALSE) {
  UseMethod("geoarrow_create")
}

#' @rdname geoarrow_create
#' @export
geoarrow_create.default <- function(handleable, ..., schema = geoarrow_schema_default(handleable),
                                    strict = FALSE) {
  extension <- scalar_chr(schema$metadata[["ARROW:extension:name"]])

  if (identical(extension, "geoarrow::wkt")) {
    return(geoarrow_create_wkt_array(unclass(wk::as_wkt(handleable)), schema))
  } else if (identical(extension, "geoarrow::geojson")) {
    if (!requireNamespace("geos", quietly = TRUE) || (geos::geos_version() < "3.10")) {
      # nocov start
      stop(
        "Package 'geos' with 'libgeos' >= 3.10 required to export handleable as GeoJSON",
        call. = FALSE
      )
      # nocov end
    }

    geos_geom <- geos::as_geos_geometry(handleable)
    geojson_geom <- geos::geos_write_geojson(geos_geom)

    return(geoarrow_create_geojson_array(geojson_geom, schema, strict = strict))
  } else if (identical(extension, "geoarrow::wkb")) {
    return(geoarrow_create_wkb_array(unclass(wk::as_wkb(handleable)), schema, strict = strict))
  }

  # Eventually this will be done with dedicated wk handlers at the C level with
  # minimal allocs. For now, this is going to generate a lot of copying.

  # all types need the coordinates in flat form
  coords <- wk::wk_coords(handleable)

  if (identical(extension, "geoarrow::point")) {
    return(geoarrow_create_point_array(coords, schema))
  }

  # everything except point needs counts
  counts <- wk::wk_count(handleable)

  if (identical(extension, "geoarrow::linestring")) {
    return(geoarrow_create_linestring_array(coords, counts$n_coord, schema, strict = strict))
  } else if (identical(extension, "geoarrow::polygon")) {
    # wk_count() doesn't do coordinate count for rings, but we can use
    # rle() for this because rings are never empty or null
    ring_coord_counts <- rle(coords$ring_id)$lengths

    array <- geoarrow_create_polygon_array(
      coords,
      lengths = list(counts$n_ring, ring_coord_counts),
      schema = schema,
      strict = strict
    )

    return(array)

  } else if (identical(extension, "geoarrow::multi")) {
    child_schema <- schema$children[[1]]
    sub_extension <- scalar_chr(child_schema$metadata[["ARROW:extension:name"]])

    if (identical(sub_extension, "geoarrow::point")) {
      return(geoarrow_create_multipoint_array(coords, counts$n_coord, schema))
    } else if (identical(sub_extension, "geoarrow::linestring")) {
      flat_counts <- wk::wk_handle(
        handleable,
        wk::wk_flatten_filter(
          wk::wk_count_handler()
        )
      )

      array <- geoarrow_create_multilinestring_array(
        coords,
        lengths = list(counts$n_geom - 1L, flat_counts$n_coord),
        schema = schema,
        strict = strict
      )

      return(array)
    } else if (identical(sub_extension, "geoarrow::polygon")) {
      flat_counts <- wk::wk_handle(
        handleable,
        wk::wk_flatten_filter(
          wk::wk_count_handler(),
          add_details = TRUE
        )
      )
      geom_counts <- rle(attr(flat_counts, "wk_details")$feature_id)$lengths
      ring_coord_counts <- rle(coords$ring_id)$lengths

      array <- geoarrow_create_multipolygon_array(
        coords,
        lengths = list(geom_counts, flat_counts$n_ring, ring_coord_counts),
        schema = schema,
        strict = strict
      )

      return(array)
    } else {
      stop(
        sprintf("Extension '%s' not supported within 'geoarrow::multi'", sub_extension),
        call. = FALSE
      )
    }
  }


  stop(
    sprintf("Extension '%s' not supported by geoarrow_create()", extension),
    call. = FALSE
  )
}

geoarrow_create_wkt_array <- function(x, schema, strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::wkt")
  )

  geoarrow_create_string_array(x, schema, strict = strict)
}

geoarrow_create_geojson_array <- function(x, schema, strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::geojson")
  )

  geoarrow_create_string_array(x, schema, strict = strict)
}

geoarrow_create_wkb_array <- function(x, schema, strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::wkb")
  )

  nullable <- bitwAnd(schema$flags, carrow::carrow_schema_flags(nullable = TRUE)) != 0
  is_na <- vapply(x, is.null, logical(1))
  if (nullable) {
    null_count <- sum(is_na)
    validity_buffer <- if (null_count > 0) carrow::as_carrow_bitmask(!is_na) else NULL
  } else {
    stopifnot(all(!is_na))
    validity_buffer <- NULL
    null_count <- 0
  }

  item_lengths <- as.numeric(lengths(x, use.names = FALSE))
  flat <- unlist(x, use.names = FALSE)
  total_length <- length(flat)

  if (!strict) {
    unique_lengths <- unique(item_lengths)
    if (length(unique_lengths) == 1L) {
      schema$format <- sprintf("w:%d", unique_lengths)
    } else if (total_length > ((2 ^ 31) - 1)) {
      schema$format <- "Z"
    }
  }

  if (identical(schema$format, "z")) {
    array_data <- carrow::carrow_array_data(
      length = length(x),
      buffers = list(
        validity_buffer,
        as.integer(c(0L, cumsum(item_lengths))),
        flat
      ),
      null_count = null_count
    )

    carrow::carrow_array(schema, array_data)
  } else if (identical(schema$format, "Z")) {
    array_data <- carrow::carrow_array_data(
      length = length(x),
      buffers = list(
        validity_buffer,
        carrow::as_carrow_int64(c(0L, cumsum(item_lengths))),
        flat
      ),
      null_count = null_count
    )

    carrow::carrow_array(schema, array_data)
  } else if (startsWith(schema$format, "w:")) {
    width <- carrow::parse_format(schema$format)$args$n_bytes
    stopifnot(all(item_lengths == width))

    array_data <- carrow::carrow_array_data(
      length = length(x),
      buffers = list(
        validity_buffer,
        flat
      ),
      null_count = null_count
    )

    carrow::carrow_array(schema, array_data, validate = FALSE)
  } else {
    stop(
      sprintf("Unsupported binary encoding format: '%s'", schema$format),
      call. = FALSE
    )
  }
}

geoarrow_create_multipolygon_array <- function(coords, lengths, schema,
                                                 n = lapply(lengths, length),
                                                 strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::multi"),
    length(lengths) == 3,
    length(n) == 3
  )

  geoarrow_create_nested_list(
    lengths[[1]],
    schema,
    n = n[[1]],
    make_child_array = geoarrow_create_polygon_array,
    list(
      coords,
      lengths[-1],
      n = n[-1]
    ),
    strict = strict
  )
}

geoarrow_create_multilinestring_array <- function(coords, lengths, schema,
                                                  n = lapply(lengths, length),
                                                  strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::multi"),
    length(lengths) == 2,
    length(n) == 2
  )

  geoarrow_create_nested_list(
    lengths[[1]],
    schema,
    n = n[[1]],
    make_child_array = geoarrow_create_linestring_array,
    list(
      coords,
      lengths[[2]],
      n = n[[2]]
    ),
    strict = strict
  )
}

geoarrow_create_multipoint_array <- function(coords, lengths, schema, n = length(lengths),
                                             strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::multi")
  )

  geoarrow_create_nested_list(
    lengths,
    schema,
    n = n,
    make_child_array = geoarrow_create_point_array,
    list(coords),
    strict = strict
  )
}

geoarrow_create_polygon_array <- function(coords, lengths, schema,
                                          n = lapply(lengths, length),
                                          strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::polygon")
  )

  geoarrow_create_nested_list(
    lengths[[1]],
    schema,
    n = n[[1]],
    make_child_array = geoarrow_create_nested_list,
    list(
      lengths[[2]],
      n = n[[2]],
      make_child_array = geoarrow_create_point_array,
      list(coords)
    ),
    strict = strict
  )
}

geoarrow_create_linestring_array <- function(coords, lengths, schema, n = length(lengths),
                                             strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::linestring")
  )

  geoarrow_create_nested_list(
    lengths,
    schema,
    n = n,
    make_child_array = geoarrow_create_point_array,
    list(coords),
    strict = strict
  )
}

geoarrow_create_nested_list <- function(lengths, schema, n, make_child_array, args, strict = FALSE) {
  nullable <- bitwAnd(schema$flags, carrow::carrow_schema_flags(nullable = TRUE)) != 0
  if (nullable) {
    is_na <- is.na(lengths)
    null_count <- sum(is_na)
    validity_buffer <- if (null_count > 0) carrow::as_carrow_bitmask(!is_na) else NULL
  } else {
    validity_buffer <- NULL
    null_count <- 0
  }

  lengths_finite <- lengths
  lengths_finite[is.na(lengths_finite)] <- 0L

  total_length <- sum(lengths_finite)
  if (typeof(total_length) == "double") {
    # or else cumsum() may give an integer overflow
    lengths_finite <- as.double(lengths_finite)
  }

  if (!strict) {
    unique_lengths <- unique(lengths_finite)
    if (length(unique_lengths) == 1L) {
      schema$format <- sprintf("+w:%d", unique_lengths)
    } else if (total_length > ((2 ^ 31) - 1)) {
      schema$format <- "+L"
    }
  }

  if (identical(schema$format, "+l")) {
    stopifnot(total_length < (2 ^ 31))

    child_array <- do.call(make_child_array, c(args, list(schema = schema$children[[1]], strict = strict)))
    schema$children[[1]] <- child_array$schema
    stopifnot(as.numeric(child_array$array_data$length) >= total_length)
    offsets <- c(0L, cumsum(lengths_finite))

    carrow::carrow_array(
      schema,
      carrow::carrow_array_data(
        buffers = list(
          validity_buffer,
          as.integer(offsets)
        ),
        length = n,
        null_count = null_count,
        children = list(
          child_array$array_data
        )
      )
    )
  } else if (identical(schema$format, "+L")) {
    child_array <- do.call(make_child_array, c(args, list(schema = schema$children[[1]], strict = strict)))
    schema$children[[1]] <- child_array$schema
    stopifnot(as.numeric(child_array$array_data$length) >= total_length)
    offsets <- c(0L, cumsum(lengths_finite))

    carrow::carrow_array(
      schema,
      carrow::carrow_array_data(
        buffers = list(
          validity_buffer,
          carrow::as_carrow_int64(offsets)
        ),
        length = n,
        null_count = null_count,
        children = list(
          child_array$array_data
        )
      )
    )
  } else if (startsWith(schema$format, "+w:")) {
    width <- carrow::parse_format(schema$format)$args$n_items
    stopifnot(
      all(lengths == width, na.rm = TRUE),
      is.null(validity_buffer) || (length(lengths) == n),
      !is.null(validity_buffer) || all(is.finite(lengths))
    )

    child_array <- do.call(make_child_array, c(args, list(schema = schema$children[[1]], strict = strict)))
    schema$children[[1]] <- child_array$schema
    stopifnot(
      as.numeric(child_array$array_data$length) >= (width * n)
    )

    carrow::carrow_array(
      schema,
      carrow::carrow_array_data(
        buffers = list(
          validity_buffer
        ),
        length = n,
        null_count = null_count,
        children = list(
          child_array$array_data
        )
      ),
      validate = FALSE
    )
  } else {
    stop(
      sprintf("Unsupported nested list storage type '%s'", schema$format),
      call. = FALSE
    )
  }
}

geoarrow_create_point_array <- function(coords, schema, strict = FALSE) {
  stopifnot(
    identical(schema$metadata[["ARROW:extension:name"]], "geoarrow::point")
  )
  geoarrow_meta <- geoarrow_metadata(schema)

  stopifnot(
    !is.null(geoarrow_meta$dim),
    geoarrow_meta$dim %in% c("xy", "xyz", "xym", "xyzm")
  )

  # so that you can pass an xy() or list(x, y)
  coords <- unclass(coords)

  dims_in_schema <- strsplit(geoarrow_meta$dim, "")[[1]]
  dims_in_coords <- intersect(names(coords), c("x", "y", "z", "m"))

  # fills dimensions that aren't specified with NA
  coords_dim <- unclass(
    wk::as_xy(
      wk::new_wk_xy(coords[dims_in_coords]),
      dims = dims_in_schema
    )
  )

  stopifnot(identical(names(coords_dim), dims_in_schema))
  n_dim <- length(coords_dim)
  n_coord <- length(coords_dim[[1]])

  nullable <- bitwAnd(schema$flags, carrow::carrow_schema_flags(nullable = TRUE)) != 0
  if (nullable) {
    is_na <- Reduce("&", lapply(coords_dim, is.na))
    null_count <- sum(is_na)
    validity_buffer <- if (null_count > 0) carrow::as_carrow_bitmask(!is_na) else NULL
  } else {
    # here NAs are OK because they're considered NaN in double math
    validity_buffer <- NULL
    null_count <- 0
  }

  if (identical(schema$format, "+s")) {
    struct_names <- vapply(schema$children, function(x) x$name, character(1))
    struct_formats <- vapply(schema$children, function(x) x$format, character(1))
    stopifnot(
      identical(unname(struct_names), names(coords_dim)),
      # not supporting float32 yet
      all(struct_formats == "g")
    )

    carrow::carrow_array(
      schema,
      carrow::carrow_array_data(
        buffers = list(validity_buffer),
        length = n_coord,
        children = lapply(coords_dim, function(x) {
          carrow::carrow_array_data(
            buffers = list(NULL, x),
            length = n_coord,
            null_count = 0
          )
        }),
        null_count = null_count
      )
    )
  } else if (startsWith(schema$format, "+w")) {
    width <- carrow::parse_format(schema$format)$args$n_items
    stopifnot(
      identical(as.integer(width), as.integer(n_dim))
    )

    # probably doesn't support more than 2 ^ 31 - 1 coordinates
    interleaved <- do.call(rbind, unname(coords_dim))

    carrow::carrow_array(
      schema,
      carrow::carrow_array_data(
        length = ncol(interleaved),
        buffers = list(validity_buffer),
        children = list(
          carrow::carrow_array_data(
            buffers = list(NULL, interleaved),
            length = length(interleaved),
            null_count = 0
          )
        ),
        null_count = null_count
      ),
      validate = FALSE
    )
  } else {
    stop(sprintf("Unsupported point storage type '%s'", schema$format), call. = FALSE)
  }
}

geoarrow_create_string_array <- function(x, schema, strict = FALSE) {
  array <- carrow::as_carrow_array(x)

  if (!strict || identical(array$schema$format, schema$format)) {
    schema$format <- array$schema$format
    array$schema <- schema
    return(array)
  }

  # we can totally convert a regular unicode to large unicode
  if (identical(schema$format, "U")) {
    array <- unclass(array)
    array$schema <- schema

    # carrow makes it hard to mess with the buffers (on purpose)
    array$array_data <- carrow::carrow_array_data_info(array$array_data)
    array$array_data$buffers <- list(
      array$array_data$buffers[[1]],
      carrow::as_carrow_int64(array$array_data$buffers[[2]]),
      array$array_data$buffers[[3]]
    )
    array$array_data$n_buffers <- NULL
    array$array_data$n_children <- NULL
    array$array_data <- do.call(carrow::carrow_array_data, array$array_data)

    return(do.call(carrow::carrow_array, array))
  }

  stop(
    "Attempt to create small unicode array with > (2 ^ 31 - 1) elements",
    call. = FALSE
  )
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
    } else {
      vector_meta$geometry_type <- wk::wk_geometry_type("geometrycollection")
    }
  }

  point_metadata <- geoarrow_metadata(point)

  if (is.null(point_metadata$crs)) {
    crs <- wk::wk_crs(handleable)
    if (!is.null(crs) && !inherits(crs, "wk_crs_inherit")) {
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
    # for now, fall back to WKB for collections or mixed types
    geoarrow_schema_wkb(),
    # geoarrow_schema_sparse_geometrycollection(
    #   children = lapply(
    #     all_geometry_types,
    #     geoarrow_schema_default_base,
    #     all_geometry_types = NULL,
    #     point = point
    #   )
    # ),
    stop(sprintf("Unsupported geometry type ID '%d'", geometry_type), call. = FALSE) # nocov
  )
}
