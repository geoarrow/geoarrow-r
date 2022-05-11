
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
  is_slice(x) && (x[1] == 1L) && (x[length(x)] == len)
}

#' @importFrom narrow as_narrow_array_stream
#' @export
as_narrow_array_stream.geoarrow_vctr <- function(x, ...) {
  schema <- attr(x, "schema", exact = TRUE)
  array_data <- attr(x, "array_data", exact = TRUE)

  if (length(array_data) == 1) {
    arrays <- list(narrow::as_narrow_array(x))
  } else {
    arrays <- vector("list", length(array_data))
    length <- 0L
    for (i in seq_along(arrays)) {
      arrays[[i]] <- geoarrow_compute(
        narrow::narrow_array(schema, array_data[[i]]),
        "cast",
        list(schema = schema),
        filter = x - length
      )

      length <- length + array_data[[i]]$length
    }
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
      stream,
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
  indices <- rep_len(vctr_indices(x), ...)
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
  format(x)
}
