
#' Create GeoArrow Vectors
#'
#' The 'geoarrow_vctr' family of vector classes are a zero-copy wrapper around
#' an Arrow C Data interface representation of a [arrow::ChunkedArray]. This
#' class provides most of the features that you might want when loading a file
#' (e.g., subset, print, format, inspect), and can be converted to some other
#' vector class (e.g., using [sf::st_as_sf()] or [geoarrow_collect_sf()]) with
#' more features when appropriate.
#'
#' @param x An object that implements [wk::wk_handle()]
#' @param ... Passed to S3 methods
#' @param ptype A prototype for the final vctr type, or NULL to guess the most
#'   appropriate type.
#'
#' @return A vector class inheriting from 'geoarrow_vctr'.
#' @export
#'
geoarrow <- function(x = wk::wkb(crs = NULL)) {
  schema <- geoarrow_schema_default(x)

  geoarrow_vctr(
    x,
    geoarrow_schema_default(x)
  )
}

#' @rdname geoarrow
#' @export
geoarrow_wkb <- function(x = wk::wkb(crs = NULL)) {
  geoarrow_vctr(
    x,
    geoarrow_schema_wkb(
      crs = wk::wk_crs(x),
      edges = if (wk::wk_is_geodesic(x)) "spherical"
    )
  )
}

#' @rdname geoarrow
#' @export
geoarrow_wkt <- function(x = wk::wkb(crs = NULL)) {
  geoarrow_vctr(
    x,
    geoarrow_schema_wkt(
      crs = wk::wk_crs(x),
      edges = if (wk::wk_is_geodesic(x)) "spherical"
    )
  )
}

#' @rdname geoarrow
#' @export
as_geoarrow <- function(x, ..., ptype = NULL) {
  UseMethod("as_geoarrow")
}

#' @rdname geoarrow
#' @export
as_geoarrow.geoarrow_vctr <- function(x, ..., ptype = NULL) {
  if (is.null(ptype)) {
    x
  } else {
    NextMethod()
  }
}

#' @export
as_geoarrow.default <- function(x, ..., ptype = NULL) {
  if (is.null(ptype)) {
    schema <- geoarrow_schema_default(x)
  } else {
    schema <- narrow::as_narrow_schema(ptype)
  }

  geoarrow_vctr(x, schema)
}

#' @export
as_geoarrow.narrow_array <- function(x, ..., ptype = NULL) {
  if (is.null(ptype)) {
    cls <- gsub("\\.", "_", x$schema$metadata[["ARROW:extension:name"]])
    stopifnot(grepl("^geoarrow_", cls))

    new_geoarrow_vctr(
      x$schema,
      list(x$array_data),
      cls
    )
  } else {
    NextMethod()
  }
}

geoarrow_vctr <- function(x, schema) {
  # opportunity to short-circuit if we already have a geoarrow vctr
  if (inherits(x, "geoarrow_vctr")) {
    schema_proxy <- narrow::narrow_schema_info(schema, recursive = TRUE)
    x_schema_proxy <- narrow::narrow_schema_info(
      attr(x, "schema", exact = TRUE),
      recursive = TRUE
    )

    if (identical(schema_proxy, x_schema_proxy)) {
      return(x)
    }
  }


  narrow_array <- geoarrow_create_narrow(
    x,
    schema = schema,
    strict = TRUE
  )

  cls <- gsub("\\.", "_", schema$metadata[["ARROW:extension:name"]])
  stopifnot(grepl("^geoarrow_", cls))

  new_geoarrow_vctr(
    narrow_array$schema,
    list(narrow_array$array_data),
    cls
  )
}

new_geoarrow_vctr <- function(schema, array_data, subclass, indices = NULL) {
  stopifnot(inherits(schema, "narrow_schema"))
  stopifnot(all(vapply(array_data, inherits, logical(1), "narrow_array_data")))

  structure(
    indices %||%
      seq_len(sum(vapply(array_data, "[[", integer(1), "length"))),
    schema = schema,
    array_data = array_data,
    class = union(subclass, "geoarrow_vctr")
  )
}

vctr_proxy <- function(x, ...) {
  x
}

vctr_restore <- function(x, to, ...) {
  new_geoarrow_vctr(
    attr(to, "schema", exact = TRUE),
    attr(to, "array_data", exact = TRUE),
    class(to),
    indices = x
  )
}

is_slice <- function(x) {
  .Call(geoarrow_c_is_slice, x)
}

is_identity_slice <- function(x, len) {
  .Call(geoarrow_c_is_identity_slice, x, as.integer(len))
}

all_increasing <- function(x) {
  indices <- unclass(x)
  all(diff(indices[!is.na(indices)]) >= 0)
}

#' @importFrom narrow as_narrow_schema
#' @export
as_narrow_schema.geoarrow_vctr <- function(x, ...) {
  attr(x, "schema", exact = TRUE)
}


#' @importFrom narrow as_narrow_array_stream
#' @export
as_narrow_array_stream.geoarrow_vctr <- function(x, ...) {
  schema <- attr(x, "schema", exact = TRUE)
  array_data <- attr(x, "array_data", exact = TRUE)

  # We do need at least one array to work with, even if it's empty
  if (length(array_data) == 0) {
    array_data <- list(
      wk::wk_handle(
        narrow::narrow_array_stream(list(), schema = schema),
        geoarrow_compute_handler("cast", list(schema = schema, strict = TRUE))
      )$array_data
    )
  }

  if (length(x) == 0) {
    arrays <- list()
  } else if (length(array_data) == 1) {
    # with a single array, the as_narrow_array() method can handle conversion
    arrays <- list(narrow::as_narrow_array(x))
  } else if (all_increasing(x)) {
    # As long as we have strictly increasing or NA indices, we can
    # use compute + cast + filter to resolve each array. This probably happens
    # after conversion from ChunkedArray where a filter() or slice() or
    # group_by() has occurred.
    arrays <- vector("list", length(array_data))

    array_index_begin <- 0L
    array_index_end <- 0L
    first_index <- 0L
    last_index <- 0L

    indices <- unclass(x)
    index_index <- seq_along(indices)

    for (i in seq_along(arrays)) {
      array_index_end <- array_index_end + array_data[[i]]$length

      first_index <- last_index + 1L
      index_filter <- index_index >= first_index &
        is.finite(indices) & indices <= array_index_end
      if (!any(index_filter)) {
        next
      }
      last_index <- max(which(index_filter))

      arrays[[i]] <- geoarrow_compute(
        narrow::narrow_array(schema, array_data[[i]]),
        "cast",
        list(schema = schema, strict = TRUE),
        filter = indices[first_index:last_index] - array_index_begin
      )

      array_index_begin <- array_index_begin + array_data[[i]]$length
    }

    # there still could be some pending NAs at the end of the index list
    if (last_index != length(indices)) {
      arrays[[length(arrays) + 1]] <- geoarrow_compute(
        narrow::narrow_array(schema, array_data[[1]]),
        "cast",
        list(schema = schema, strict = TRUE),
        filter = rep_len(NA_integer_, length(indices) - last_index)
      )
    }

    arrays <- arrays[!vapply(arrays, is.null, logical(1))]
  } else {
    # This is where the user has requested a reordered version of the vctr,
    # most likely because it was a ChunkedArray that became a geoarrow_vctr
    # that is getting rearranged along another variable. Punt this to Arrow
    # for now.
    stopifnot(has_arrow_with_extension_type())

    # (concatenate with extension arrays not supported, so we have to do this
    # with the storage chunked array...)
    chunked_array <- arrow::as_chunked_array(x)
    storage_arrays <- lapply(chunked_array$chunks, function(chunk) chunk$storage())
    storage_chunked_array <- arrow::chunked_array(!!! storage_arrays)
    result_chunked <- storage_chunked_array$Take(unclass(x) - 1L)
    stopifnot(result_chunked$num_chunks == 1L)
    result_narrow <- narrow::as_narrow_array(result_chunked$chunk(0))
    result_narrow$schema <- attr(x, "schema", exact = TRUE)

    arrays <- list(result_narrow)
  }

  narrow::narrow_array_stream(arrays, schema = schema)
}

#' @importFrom narrow as_narrow_array
#' @export
as_narrow_array.geoarrow_vctr <- function(x, ...) {
  schema <- attr(x, "schema", exact = TRUE)
  array_data <- attr(x, "array_data", exact = TRUE)

  # Simpler if we always have an array to work with
  if (length(array_data) == 0) {
    attr(x, "array_data") <- list(
      wk::wk_handle(
        narrow::narrow_array_stream(list(), schema = schema),
        geoarrow_compute_handler("cast", list(schema = schema, strict = TRUE))
      )$array_data
    )

    array_data <- attr(x, "array_data", exact = TRUE)
  }

  if (length(array_data) == 1 && is_identity_slice(x, array_data[[1]]$length)) {
    # freshly constructed!
    narrow::narrow_array(schema, array_data[[1]])
  } else if (length(array_data) == 1) {
    # subset
    geoarrow_compute(
      narrow::narrow_array(schema, array_data[[1]]),
      "cast",
      list(schema = schema, strict = TRUE),
      filter = x
    )
  } else {
    # concatenation
    wk::wk_handle(
      narrow::as_narrow_array_stream(x),
      geoarrow_compute_handler("cast", list(schema = schema, strict = TRUE))
    )
  }
}

#' @export
format.geoarrow_vctr <- function(x, ...) {
  wk::wk_format(x)
}

#' @export
print.geoarrow_vctr <- function(x, ...) {
  cat(sprintf("<%s[%s]>\n", class(x)[1], length(x)))

  if (length(x) == 0) {
    return(invisible(x))
  }

  max_print <- getOption("max.print", 1000)

  x_head <- utils::head(x, max_print)
  formatted <- format(x_head)
  formatted <- stats::setNames(formatted, names(x_head))
  print(formatted, ..., quote = FALSE)

  if (length(x) > max_print) {
    cat(sprintf("Reached max.print (%s)\n", max_print))
  }

  invisible(x)
}

#' @export
str.geoarrow_vctr <- function(object, ..., indent.str = "", width = getOption("width")) {
  if (length(object) == 0) {
    cat(paste0(" ", class(object)[1], "[0]\n"))
    return(invisible(object))
  }

  # estimate possible number of elements that could be displayed
  # to avoid formatting too many
  width <- width - nchar(indent.str) - 2
  length <- min(length(object), ceiling(width / 5))

  formatted <- format(utils::head(object, length), trim = TRUE)

  title <- paste0(" ", class(object)[1], "[1:", length(object), "]")
  cat(
    paste0(
      title,
      " ",
      strtrim(paste0(formatted, collapse = " "), width - nchar(title)),
      "\n"
    )
  )
  invisible(object)
}

#' @export
is.na.geoarrow_vctr <- function(x) {
  schema <- attr(x, "schema", exact = TRUE)
  array_data <- attr(x, "array_data", exact = TRUE)

  index_na <- is.na(unclass(x))
  any_array_data_na <- FALSE
  for(array_datum in array_data) {
    if (array_datum$null_count != 0) {
      any_array_data_na <- TRUE
      break
    }
  }

  if (any_array_data_na) {
    index_na | is.na(wk::wk_meta(x)$geometry_type)
  } else {
    index_na
  }
}

#' @export
`[.geoarrow_vctr` <- function(x, i) {
  vctr_restore(NextMethod(), x)
}

#' @export
`[[.geoarrow_vctr` <- function(x, i) {
  x[i]
}

#' @export
`[<-.geoarrow_vctr` <- function(x, i, value) {
  stop("Subset-assign is not supported for geoarrow_vctr")
}

#' @export
`[[<-.geoarrow_vctr` <- function(x, i, value) {
  x[i] <- value
  x
}

#' @export
c.geoarrow_vctr <- function(...) {
  dots <- list(...)

  if (length(dots) == 1L) {
    return(dots[[1]])
  }

  ptype_out <- Reduce(geoarrow_ptype2, dots)

  schema_out <- attr(ptype_out, "schema", exact = TRUE)
  cls <- gsub("\\.", "_", schema_out$metadata[["ARROW:extension:name"]])
  dots_casted <- lapply(dots, geoarrow_vctr, schema_out)
  array_data <- do.call(c, lapply(dots_casted, attr, "array_data"))
  length <- sum(vapply(array_data, "[[", integer(1), "length"))

  new_geoarrow_vctr(
    schema_out,
    array_data,
    cls,
    seq_len(length)
  )
}

geoarrow_ptype2 <- function(x, y) {
  # eventually we can do more complex things here, but for now we just
  # convert to WKB unless the schemas are identical because this involves
  # the fewest assumptions
  schema_x <- attr(x, "schema", exact = TRUE)
  schema_y <- attr(y, "schema", exact = TRUE)

  schemas_identical <- !is.null(schema_x) &&
    !is.null(schema_y) &&
    identical(
      narrow::narrow_schema_info(schema_x, recursive = TRUE),
      narrow::narrow_schema_info(schema_y, recursive = TRUE)
    )

  if (schemas_identical) {
    new_geoarrow_vctr(schema_x, list(), class(x), integer())
  } else {
    # will error for incompatible crses, edges
    crs_out <- wk::wk_crs_output(x, y)
    geodesic_out <- wk::wk_is_geodesic_output(x, y)

    new_geoarrow_vctr(
      geoarrow_schema_wkb(
        crs = wk::wk_crs(x),
        edges = if (wk::wk_is_geodesic(x)) "spherical"
      ),
      list(),
      "geoarrow_wkb",
      integer()
    )
  }
}

#' @export
rep.geoarrow_vctr <- function(x, ...) {
  vctr_restore(NextMethod(), x)
}

#' @method rep_len geoarrow_vctr
#' @export
rep_len.geoarrow_vctr <- function(x, ...) {
  indices <- rep_len(unclass(x), ...)
  vctr_restore(indices, x)
}

# data.frame() will call as.data.frame() with optional = TRUE
#' @export
as.data.frame.geoarrow_vctr <- function(x, ..., optional = FALSE) {
  if (!optional) {
    NextMethod()
  } else {
    new_data_frame(list(x))
  }
}

# ...because of the RStudio Viewer
#' @export
as.character.geoarrow_vctr <- function(x, ...) {
  formatted <- format(x)
  formatted[is.na(x)] <- NA_character_
  formatted
}
