
geoarrow_kernel_call_scalar <- function(kernel_name, stream, options = NULL, n = Inf) {
  if (inherits(stream, "nanoarrow_array")) {
    stream <- nanoarrow::basic_array_stream(list(stream))
  } else {
    stream <- nanoarrow::as_nanoarrow_array_stream(stream)
  }

  schema_in <- stream$get_schema()
  kernel <- geoarrow_kernel(kernel_name, list(schema_in), options)

  batches <- vector("list", 1024L)
  n_batches <- 0
  get_next <- stream$get_next
  while (n_batches < n) {
    array_in <- get_next(schema_in, validate = FALSE)
    if (is.null(array_in)) {
      break
    }

    n_batches <- n_batches + 1
    batches[[n_batches]] <- geoarrow_kernel_push(kernel, list(array_in))
  }

  schema <- attr(kernel, "output_type")
  nanoarrow::basic_array_stream(
    batches[seq_len(n_batches)],
    schema,
    validate = FALSE
  )
}

geoarrow_kernel_call_agg <- function(kernel_name, stream, options = NULL, n = Inf) {
  if (inherits(stream, "nanoarrow_array")) {
    stream <- nanoarrow::basic_array_stream(list(stream))
  } else {
    stream <- nanoarrow::as_nanoarrow_array_stream(stream)
  }

  schema_in <- stream$get_schema()
  kernel <- geoarrow_kernel(kernel_name, list(schema_in), options)

  get_next <- stream$get_next
  n_batches <- 0
  while (n_batches < n) {
    array_in <- get_next(schema_in, validate = FALSE)
    if (is.null(array_in)) {
      break
    }

    geoarrow_kernel_push(kernel, list(array_in))
    n_batches <- n_batches + 1
  }

  geoarrow_kernel_finish(kernel)
}

geoarrow_kernel <- function(kernel_name, input_types, options = NULL) {
  kernel_name <- as.character(kernel_name)[1]
  input_types <- lapply(input_types, nanoarrow::as_nanoarrow_schema)
  options_raw <- serialize_kernel_options(options)
  schema_out <- nanoarrow::nanoarrow_allocate_schema()

  kernel <- .Call(geoarrow_c_kernel, kernel_name, input_types, options_raw, schema_out)

  structure(
    kernel,
    class = c(paste0("geoarrow_kernel_", kernel_name), "geoarrow_kernel"),
    input_types = input_types,
    options = options,
    output_type = schema_out,
    is_agg = endsWith(kernel_name, "_agg")
  )
}

geoarrow_kernel_push <- function(kernel, args) {
  stopifnot(inherits(kernel, "geoarrow_kernel"))

  if (isTRUE(attr(kernel, "is_agg"))) {
    array_out <- NULL
  } else {
    array_out <- nanoarrow::nanoarrow_allocate_array()
    nanoarrow::nanoarrow_array_set_schema(
      array_out,
      attr(kernel, "output_type"),
      validate = FALSE
    )
  }

  args <- lapply(args, nanoarrow::as_nanoarrow_array)
  expected_arg_count <- length(attr(kernel, "input_types"))
  stopifnot(length(args) == expected_arg_count)

  .Call(geoarrow_c_kernel_push, kernel, args, array_out)
  array_out
}

geoarrow_kernel_finish <- function(kernel) {
  array_out <- nanoarrow::nanoarrow_allocate_array()
  nanoarrow::nanoarrow_array_set_schema(
    array_out,
    attr(kernel, "output_type"),
    validate = FALSE
  )

  .Call(geoarrow_c_kernel_finish, kernel, array_out)
  array_out
}

serialize_kernel_options <- function(vals) {
  vals <- vals[!vapply(vals, is.null, logical(1))]
  vals <- vapply(vals, as.character, character(1))

  if (length(vals) == 0) {
    return(as.raw(c(0x00, 0x00, 0x00, 0x00)))
  }

  # When this matters we can wire up nanoarrow's serializer
  tmp <- tempfile()
  con <- file(tmp, open = "w+b")
  on.exit({
    close(con)
    unlink(tmp)
  })

  writeBin(length(vals), con, size = 4L)
  for (i in seq_along(vals)) {
    key <- charToRaw(enc2utf8(names(vals)[i]))
    value <- charToRaw(enc2utf8(vals[[i]]))

    writeBin(length(key), con, size = 4L)
    writeBin(key, con)
    writeBin(length(value), con, size = 4L)
    writeBin(value, con)
  }

  n_bytes <- seek(con)
  seek(con, 0L)
  readBin(con, what = raw(), n = n_bytes)
}
