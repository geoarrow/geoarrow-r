
#' Create GeoArrow Vectors
#'
#' @param x An object that implements [wk::wk_handle()]
#'
#' @return A vector class inheriting from 'geoarrow_vctr'.
#' @export
#'
geoarrow <- function(x = wk::wkb()) {
  schema <- geoarrow_schema_default(x)

  geoarrow_vctr(
    x,
    geoarrow_schema_default(x)
  )
}

#' @rdname geoarrow
#' @export
geoarrow_wkb <- function(x = wk::wkb()) {
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
geoarrow_wkt <- function(x = wk::wkb()) {
  geoarrow_vctr(
    x,
    geoarrow_schema_wkt(
      crs = wk::wk_crs(x),
      edges = if (wk::wk_is_geodesic(x)) "spherical"
    )
  )
}

geoarrow_vctr <- function(x, schema) {
  narrow_array <- geoarrow_create_narrow(
    x,
    schema = schema,
    strict = TRUE
  )

  new_geoarrow_vctr(
    narrow_array$schema,
    list(narrow_array$array_data),
    gsub("\\.", "_", schema$metadata[["ARROW:extension:name"]])
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

vctr_normalize <- function(x) {
  array <- narrow::as_narrow_array(x)
  new_geoarrow_vctr(array$schema, list(array$array_data), class(x))
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
      )
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

    for (i in seq_along(arrays)) {
      array_index_end <- array_index_end + array_data[[i]]$length

      first_index <- last_index + 1L
      index_filter <- indices >= first_index & indices <= array_index_end
      if (!any(index_filter)) {
        next
      }
      last_index <- max(which(index_filter))

      arrays[[i]] <- geoarrow_compute(
        narrow::narrow_array(schema, array_data[[i]]),
        "cast",
        list(schema = schema),
        filter = indices[first_index:last_index] - array_index_begin
      )

      array_index_begin <- array_index_begin + array_data[[i]]$length
    }

    arrays <- arrays[!vapply(arrays, is.null, logical(1))]
  } else {
    # This is where the user has requested a reordered version of the vctr,
    # most likely because it was a ChunkedArray that became a geoarrow_vctr
    # that is getting rearranged along another variable. Punt this to Arrow
    # for now.
    stop("Scrambled multi-chunk geoarrow_vctr not implemented")
  }

  narrow::narrow_array_stream(arrays, schema = schema)
}

#' @importFrom narrow as_narrow_array
#' @export
as_narrow_array.geoarrow_vctr <- function(x, ...) {
  schema <- attr(x, "schema", exact = TRUE)
  array_data <- attr(x, "array_data", exact = TRUE)

  if (length(array_data) == 1 && is_identity_slice(x, array_data[[1]]$length)) {
    # freshly constructed!
    narrow::narrow_array(schema, array_data[[1]])
  } else if (length(array_data) == 1) {
    # subset
    geoarrow_compute(
      narrow::narrow_array(schema, array_data[[1]]),
      "cast",
      list(schema = schema),
      filter = x
    )
  } else {
    # concatenation
    wk::wk_handle(
      narrow::as_narrow_array_stream(x),
      geoarrow_compute_handler("cast", list(schema = schema))
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
  stop("c() for geoarrow_vctr not supported")
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
