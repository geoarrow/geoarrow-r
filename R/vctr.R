
#' GeoArrow encoded arrays as R vectors
#'
#' @param x An object that works with [as_geoarrow_array_stream()]. Most
#'   spatial objects in R already work with this method.
#' @param ... Passed to [as_geoarrow_array_stream()]
#' @param schema An optional `schema` (e.g., [na_extension_geoarrow()]).
#'
#' @return A vctr of class 'geoarrow_vctr'
#' @export
#'
#' @examples
#' as_geoarrow_vctr("POINT (0 1)")
#'
as_geoarrow_vctr <- function(x, ..., schema = NULL) {
  if (inherits(x, "geoarrow_vctr") && is.null(schema)) {
    return(x)
  }

  stream <- as_geoarrow_array_stream(x, ..., schema = schema)
  chunks <- nanoarrow::collect_array_stream(stream, validate = FALSE)
  new_geoarrow_vctr(chunks, stream$get_schema())
}

new_geoarrow_vctr <- function(chunks, schema, indices = NULL) {
  offsets <- .Call(geoarrow_c_vctr_chunk_offsets, chunks)
  if (is.null(indices)) {
    indices <- seq_len(offsets[length(offsets)])
  }

  structure(
    indices,
    schema = schema,
    chunks = chunks,
    offsets = offsets,
    class = c("geoarrow_vctr", "wk_vctr")
  )
}

#' @export
`[.geoarrow_vctr` <- function(x, i) {
  attrs <- attributes(x)
  x <- NextMethod()

  if (is.null(vctr_as_slice(x))) {
    stop(
      "Can't subset geoarrow_vctr with non-slice (e.g., only i:j indexing is supported)"
    )
  }

  attributes(x) <- attrs
  x
}

#' @export
`[<-.geoarrow_vctr` <- function(x, i, value) {
  stop("subset assignment for geoarrow_vctr is not supported")
}

#' @export
`[[<-.geoarrow_vctr` <- function(x, i, value) {
  stop("subset assignment for geoarrow_vctr is not supported")
}

#' @export
format.geoarrow_vctr <- function(x, ..., width = NULL, digits = NULL) {
  if (is.null(width)) {
    width <- getOption("width", 100L)
  }

  width <- max(width, 20)

  if (is.null(digits)) {
    digits <- getOption("digits", 7L)
  }

  digits <- max(digits, 0)

  formatted_array <- geoarrow_kernel_call_scalar(
    "format_wkt",
    x,
    options = c(
      max_element_size_bytes = width - 10L,
      precision = digits
    ),
    n = length(attr(x, "chunks", exact = TRUE))
  )

  formatted_chr <- nanoarrow::convert_array_stream(
    formatted_array,
    character(),
    size = length(x)
  )

  sprintf("<%s>", formatted_chr)
}

# Because RStudio's viewer uses this, we want to use the potentially abbreviated
# WKT from the format method
#' @export
as.character.geoarrow_vctr <- function(x, ...) {
  format(x, ...)
}

#' @export
infer_nanoarrow_schema.geoarrow_vctr <- function(x, ...) {
  attr(x, "schema", exact = TRUE)
}

# Because zero-length vctrs are R's way of communicating "type", implement
# as_nanoarrow_schema() here so that it works in places that expect a type
#' @importFrom nanoarrow as_nanoarrow_schema
#' @export
as_nanoarrow_schema.geoarrow_vctr <- function(x, ...) {
  attr(x, "schema", exact = TRUE)
}

#' @export
as_geoarrow_array_stream.geoarrow_vctr <- function(x, ..., schema = NULL) {
  as_nanoarrow_array_stream.geoarrow_vctr(x, ..., schema = schema)
}

#' @importFrom nanoarrow as_nanoarrow_array_stream
#' @export
as_nanoarrow_array_stream.geoarrow_vctr <- function(x, ..., schema = NULL) {
  if (!is.null(schema)) {
    stream <- as_nanoarrow_array_stream(x, schema = NULL)
    return(as_geoarrow_array_stream(stream, schema = schema))
  }

  slice <- vctr_as_slice(x)
  if (is.null(slice)) {
    stop("Can't resolve non-slice geoarrow_vctr to nanoarrow_array_stream")
  }

  x_schema <- attr(x, "schema", exact = TRUE)

  # Zero-size slice can be an array stream with zero batches
  if (slice[2] == 0) {
    return(nanoarrow::basic_array_stream(list(), schema = x_schema))
  }

  # Full slice doesn't need slicing logic
  offsets <- attr(x, "offsets", exact = TRUE)
  batches <- attr(x, "chunks", exact = TRUE)
  if (slice[1] == 1 && slice[2] == max(offsets)) {
    return(
      nanoarrow::basic_array_stream(
        batches,
        schema = x_schema,
        validate = FALSE
      )
    )
  }

  # Calculate first and last slice information
  first_index <- slice[1] - 1L
  end_index <- first_index + slice[2]
  last_index <- end_index - 1L
  first_chunk_index <- vctr_resolve_chunk(first_index, offsets)
  last_chunk_index <- vctr_resolve_chunk(last_index, offsets)

  first_chunk_offset <- first_index - offsets[first_chunk_index + 1L]
  first_chunk_length <- offsets[first_chunk_index + 2L] - first_index
  last_chunk_offset <- 0L
  last_chunk_length <- end_index - offsets[last_chunk_index + 1L]

  # Calculate first and last slices
  if (first_chunk_index == last_chunk_index) {
    batch <- vctr_array_slice(
      batches[[first_chunk_index + 1L]],
      first_chunk_offset,
      last_chunk_length - first_chunk_offset
    )

    return(
      nanoarrow::basic_array_stream(
        list(batch),
        schema = x_schema,
        validate = FALSE
      )
    )
  }

  batch1 <- vctr_array_slice(
    batches[[first_chunk_index + 1L]],
    first_chunk_offset,
    first_chunk_length
  )

  batchn <- vctr_array_slice(
    batches[[last_chunk_index + 1L]],
    last_chunk_offset,
    last_chunk_length
  )

  seq_mid <- seq_len(last_chunk_index - first_chunk_index - 1)
  batch_mid <- batches[first_chunk_index + seq_mid]

  nanoarrow::basic_array_stream(
    c(
      list(batch1),
      batch_mid,
      list(batchn)
    ),
    schema = x_schema,
    validate = FALSE
  )
}


# Utilities for vctr methods

vctr_resolve_chunk <- function(x, offsets) {
  .Call(geoarrow_c_vctr_chunk_resolve, x, offsets)
}

vctr_as_slice <- function(x) {
  .Call(geoarrow_c_vctr_as_slice, x)
}

vctr_array_slice <- function(x, offset, length) {
  new_offset <- x$offset + offset
  new_length <- length
  nanoarrow::nanoarrow_array_modify(
    x,
    list(offset = new_offset, length = new_length),
    validate = FALSE
  )
}
