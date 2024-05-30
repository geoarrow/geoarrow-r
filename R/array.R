
#' Convert an object to a GeoArrow array
#'
#' @param x An object
#' @param schema A geoarrow extension schema to use as the target type
#' @param ... Passed to S3 methods
#'
#' @return A [nanoarrow_array][nanoarrow::as_nanoarrow_array].
#' @export
#'
#' @examples
#' as_geoarrow_array(wk::wkt("POINT (0 1)"))
#'
as_geoarrow_array <- function(x, ..., schema = NULL) {
  UseMethod("as_geoarrow_array")
}

#' @export
as_geoarrow_array.default <- function(x, ..., schema = NULL) {
  if (is.null(schema)) {
    schema <- infer_geoarrow_schema(x)
  } else {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
  }

  wk::wk_handle(x, geoarrow_writer(schema))
}

#' @export
as_geoarrow_array.nanoarrow_array <- function(x, ..., schema = NULL) {
  x_schema <- nanoarrow::infer_nanoarrow_schema(x)

  if (!is.null(schema)) {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
  }

  # If this is not already a geoarrow array, see if we can interpret it as one
  if (!is_geoarrow_schema(x_schema)) {
    x_schema <- as_geoarrow_schema(x_schema)

    # Try to copy metadata from requested schema if present (e.g., so that
    # a CRS or edge type can be assigned)
    request_metadata <- schema$metadata[["ARROW:extension:metadata"]]
    if (!is.null(request_metadata)) {
      x_schema$metadata[["ARROW:extension:metadata"]] <- request_metadata
    }

    # Reinterpret the array
    x <- reinterpret_array(x, x_schema)
  }


  if (is.null(schema)) {
    return(x)
  }

  # If the source and request type are the same, return x
  x_schema_parsed <- geoarrow_schema_parse(x_schema)
  schema_parsed <- geoarrow_schema_parse(schema)
  if (identical(x_schema_parsed, schema_parsed)) {
    return(x)
  }

  # Otherwise, let as_geoarrow() sort this out
  stream <- geoarrow_kernel_call_scalar(
    "as_geoarrow",
    nanoarrow::basic_array_stream(list(x), schema = x_schema, validate = FALSE),
    list("type" = schema_parsed$id)
  )

  stream$get_next()
}

#' @export
as_geoarrow_array.character <- function(x, ..., schema = NULL) {
  as_geoarrow_array(wk::new_wk_wkt(x), schema = schema)
}

#' @rdname as_geoarrow_array
#' @export
as_geoarrow_array_stream <- function(x, ..., schema = NULL) {
  UseMethod("as_geoarrow_array_stream")
}

#' @export
as_geoarrow_array_stream.default <- function(x, ..., schema = NULL) {
  if (!is.null(schema)) {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
  }

  nanoarrow::basic_array_stream(
    list(as_geoarrow_array(x, schema = schema)),
    schema = schema
  )
}

#' @export
as_geoarrow_array_stream.nanoarrow_array_stream <- function(x, ..., schema = NULL) {
  x_schema <- x$get_schema()

  if (!is.null(schema)) {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
  }

  # If this is not already a geoarrow array, see if we can interpret it as one
  if (!is_geoarrow_schema(x_schema)) {
    x_schema <- as_geoarrow_schema(x_schema)

    # Try to copy metadata from requested schema if present (e.g., so that
    # a CRS or edge type can be assigned)
    request_metadata <- schema$metadata[["ARROW:extension:metadata"]]
    if (!is.null(request_metadata)) {
      x_schema$metadata[["ARROW:extension:metadata"]] <- request_metadata
    }

    # Reinterpret the array
    x <- reinterpret_stream(x, x_schema)
  }

  # If there is no requested schema, just return the stream (i.e., don't attempt to
  # consume a stream to find its optimal output type)
  if (is.null(schema)) {
    return(x)
  }

  # If the source is the same as the destination, return it
  x_schema_parsed <- geoarrow_schema_parse(x_schema)
  schema_parsed <- geoarrow_schema_parse(schema)
  if (identical(x_schema_parsed, schema_parsed)) {
    return(x)
  }

  geoarrow_kernel_call_scalar(
    "as_geoarrow",
    reinterpret_stream(x, x_schema, validate = FALSE),
    list("type" = schema_parsed$id)
  )
}

reinterpret_array <- function(array, schema, validate = TRUE) {
  array2 <- nanoarrow::nanoarrow_allocate_array()
  nanoarrow::nanoarrow_pointer_export(array, array2)
  nanoarrow::nanoarrow_array_set_schema(array2, schema, validate = validate)
  array2
}

reinterpret_stream <- function(stream, schema, validate = TRUE) {
  # Eventually we should stream this somehow instead of collecting it all
  chunks <- nanoarrow::collect_array_stream(stream, schema = schema, validate = FALSE)
  nanoarrow::basic_array_stream(chunks, schema, validate = validate)
}

geoarrow_array_from_buffers <- function(schema, buffers) {
  schema <- nanoarrow::as_nanoarrow_schema(schema)
  extension_name <- schema$metadata[["ARROW:extension:name"]]
  stopifnot(!is.null(extension_name))

  switch(
    extension_name,
    "geoarrow.wkt" = ,
    "geoarrow.wkb" = binary_array_from_buffers(
      schema,
      offsets = buffers[[2]],
      data = buffers[[3]],
      validity = buffers[[1]]
    ),
    "geoarrow.point" = point_array_from_buffers(
      schema,
      buffers[-1],
      validity = buffers[[1]]
    ),
    "geoarrow.linestring" = ,
    "geoarrow.multipoint" = nested_array_from_buffers(
      schema,
      buffers[-1],
      level = 0,
      validity = buffers[[1]]
    ),
    "geoarrow.polygon" = ,
    "geoarrow.multilinestring" = nested_array_from_buffers(
      schema,
      buffers[-1],
      level = 1,
      validity = buffers[[1]]
    ),
    "geoarrow.multipolygon" = nested_array_from_buffers(
      schema,
      buffers[-1],
      level = 2,
      validity = buffers[[1]]
    ),
    stop(sprintf("Unhandled extension name: '%s'", extension_name))
  )
}

binary_array_from_buffers <- function(schema, offsets, data, validity = NULL) {
  is_large <- schema$format %in% c("U", "Z")

  validity <- as_validity_buffer(validity)
  buffers <- list(
    validity$buffer,
    if (is_large) as_large_offset_buffer(offsets) else as_offset_buffer(offsets),
    as_binary_buffer(data)
  )

  array <- nanoarrow::nanoarrow_array_init(schema)
  if (buffers[[2]]$size_bytes == 0) {
    return(array)
  }

  offset_element_size <- if (is_large) 8L else 4L

  nanoarrow::nanoarrow_array_modify(
    array,
    list(
      length = (buffers[[2]]$size_bytes %/% offset_element_size) - 1L,
      null_count = validity$null_count,
      buffers = buffers
    )
  )
}

point_array_from_buffers <- function(schema, buffers, validity = NULL) {
  validity <- as_validity_buffer(validity)

  ordinate_buffers <- lapply(buffers, as_coord_buffer)

  array <- nanoarrow::nanoarrow_array_init(schema)

  children <- Map(
    function(child, buffer) {
      nanoarrow::nanoarrow_array_modify(
        child,
        list(
          length = buffer$size_bytes %/% 8L,
          buffers = list(NULL, buffer)
        )
      )
    },
    array$children,
    ordinate_buffers
  )

  if (children[[1]]$length == 0) {
    return(array)
  }

  if (schema$format == "+s") {
    len_factor <- 1L
  } else {
    parsed <- nanoarrow::nanoarrow_schema_parse(schema)
    len_factor <- parsed$fixed_size
  }

  nanoarrow::nanoarrow_array_modify(
    array,
    list(
      length = ordinate_buffers[[1]]$size_bytes %/% 8L %/% len_factor,
      null_count = validity$null_count,
      buffers = list(validity$buffer),
      children = children
    )
  )
}

nested_array_from_buffers <- function(schema, buffers, level, validity = NULL) {
  if (level == 0) {
    child <- point_array_from_buffers(
      schema$children[[1]],
      buffers[-1]
    )
  } else {
    child <- nested_array_from_buffers(
      schema$children[[1]],
      buffers[-1],
      level - 1L
    )
  }

  validity <- as_validity_buffer(validity)

  array <- nanoarrow::nanoarrow_array_init(schema)
  offsets <- as_offset_buffer(buffers[[1]])
  offset_element_size <- 4L

  if (offsets$size_bytes == 0) {
    return(array)
  }

  nanoarrow::nanoarrow_array_modify(
    array,
    list(
      length = (offsets$size_bytes %/% offset_element_size) - 1L,
      null_count = validity$null_count,
      buffers = list(
        validity$buffer,
        offsets
      ),
      children = list(child)
    )
  )
}

as_binary_buffer <- function(x) {
  if (inherits(x, "nanoarrow_buffer")) {
    x
  } else if (is.raw(x)) {
    nanoarrow::as_nanoarrow_buffer(x)
  } else if (is.character(x)) {
    nanoarrow::as_nanoarrow_buffer(paste(x, collapse = ""))
  } else if (is.list(x)) {
    as_binary_buffer(unlist(x))
  } else {
    stop(
      sprintf(
        "Don't know how to create binary data buffer from object of class %s",
        class(x)[1]
      )
    )
  }
}

as_coord_buffer <- function(x) {
  if (inherits(x, "nanoarrow_buffer")) {
    return(x)
  }

  nanoarrow::as_nanoarrow_buffer(as.double(x))
}

as_offset_buffer <- function(x) {
  if (inherits(x, "nanoarrow_buffer")) {
    return(x)
  }

  nanoarrow::as_nanoarrow_buffer(as.integer(x))
}

as_large_offset_buffer <- function(x) {
  if (inherits(x, "nanoarrow_buffer")) {
    return(x)
  }

  array <- nanoarrow::as_nanoarrow_array(x, schema = nanoarrow::na_int64())
  array$buffers[[2]]
}

as_validity_buffer <- function(x) {
  if (is.null(x)) {
    return(list(null_count = 0L, buffer = NULL))
  }

  if (inherits(x, "nanoarrow_buffer")) {
    return(list(null_count = -1L, buffer = x))
  }

  if (is.logical(x)) {
    null_count <- sum(!x)
  } else {
    null_count <- sum(x == 0L)
  }

  array <- nanoarrow::as_nanoarrow_array(x, schema = nanoarrow::na_bool())
  if (array$null_count > 0) {
    stop("NA values are not allowed in validity buffer")
  }

  list(null_count = null_count, buffer = array$buffers[[2]])
}

# This really needs a helper in nanoarrow, but for now, we need a way to drop
# the extension type and convert storage only for testing
force_schema_storage <- function(schema) {
  schema$metadata[["ARROW:extension:name"]] <- NULL
  schema
}

force_array_storage <- function(array) {
  schema <- force_schema_storage(nanoarrow::infer_nanoarrow_schema(array))
  array_shallow <- nanoarrow::nanoarrow_allocate_array()
  nanoarrow::nanoarrow_pointer_export(array, array_shallow)
  nanoarrow::nanoarrow_array_set_schema(array_shallow, schema, validate = FALSE)
}
