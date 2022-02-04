
#' Read geometry from Apache Parquet files
#'
#' @param x An object to collect into a data.frame, converting geometry
#'   columns according to `handler`.
#' @param file A file or InputStream to read; passed to
#'   [arrow::read_parquet()], [arrow::read_feather()], or
#'   [arrow::read_ipc_stream()].
#' @param as_data_frame Use `FALSE` to return an [arrow::Table]
#'   instead of a data.frame.
#' @param handler A [wk handler][wk::wk_handle] to use when `as.data.frame`
#'   is TRUE for all geometry columns.
#' @param metadata Optional metadata to include to override metadata available
#'   in the file.
#' @param trans A function to be applied to each chunk after it has been
#'   collected into a data frame.
#' @inheritDotParams arrow::write_parquet
#'
#' @return The result of [arrow::read_parquet()], with geometry
#'   columns processed according to `handler`.
#'
#' @export
#'
read_geoarrow_parquet <- function(file, ..., as_data_frame = TRUE, handler = NULL,
                                  metadata = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for read_geoarrow_parquet()", call. = FALSE) # nocov
  }

  read_arrow_wrapper(
    arrow::read_parquet,
    file,
    ...,
    as_data_frame = as_data_frame,
    handler = handler,
    metadata = metadata
  )
}

#' @rdname read_geoarrow_parquet
#' @export
read_geoarrow_feather <- function(file, ..., as_data_frame = TRUE, handler = NULL,
                                  metadata = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for read_geoarrow_feather()", call. = FALSE) # nocov
  }

  read_arrow_wrapper(
    arrow::read_feather,
    file,
    ...,
    as_data_frame = as_data_frame,
    handler = handler,
    metadata = metadata
  )
}

#' @rdname read_geoarrow_parquet
#' @export
read_geoarrow_ipc_stream <- function(file, ..., as_data_frame = TRUE, handler = NULL,
                                     metadata = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for read_geoarrow_ipc_stream()", call. = FALSE) # nocov
  }

  read_arrow_wrapper(
    arrow::read_ipc_stream,
    file,
    ...,
    as_data_frame = as_data_frame,
    handler = handler,
    metadata = metadata
  )
}

#' @rdname read_geoarrow_parquet
#' @export
read_geoarrow_parquet_sf <- function(file, ...) {
  sf::st_as_sf(read_geoarrow_parquet(file, ..., handler = wk::sfc_writer))
}

#' @rdname read_geoarrow_parquet
#' @export
read_geoarrow_feather_sf <- function(file, ...) {
  sf::st_as_sf(read_geoarrow_parquet(file, ..., handler = wk::sfc_writer))
}

read_arrow_wrapper <- function(read_func, file, ..., as_data_frame = TRUE,
                               handler = NULL, metadata = NULL) {
  table <- read_func(file, ..., as_data_frame = FALSE)

  if (as_data_frame) {
    geoarrow_collect(table, handler = handler, metadata = metadata)
  } else {
    table
  }
}

#' @rdname read_geoarrow_parquet
#' @export
geoarrow_collect <- function(x, ..., handler = NULL, metadata = NULL) {
  UseMethod("geoarrow_collect")
}

#' @rdname read_geoarrow_parquet
#' @export
geoarrow_collect.Table <- function(x, ..., handler = NULL, metadata = NULL) {
  metadata <- geoarrow_object_metadata(x, metadata)

  if (is.null(metadata$columns)) {
    metadata$columns <- guess_metadata_columns(x)

    # if nothing could be guessed, fall back on x$metadata$geo
    if (length(metadata$columns) == 0) {
      geo <- x$metadata$geo
      if (is.character(geo)) {
        geo <- jsonlite::fromJSON(geo)
      }

      metadata$columns <- geo$columns
    }
  }

  handleable_cols <- intersect(names(x), names(metadata$columns))

  if (length(handleable_cols) == 0) {
    return(tibble::as_tibble(as.data.frame(x)))
  }

  handleable_results <- lapply(
    handleable_cols,
    function(col_name) {
      array_or_chunked_array <- x[[col_name]]

      if (identical(metadata$columns[[col_name]]$encoding, "::embedded::")) {
        geoarrow_schema <- narrow::as_narrow_schema(x$schema[[col_name]])
      } else {
        geoarrow_schema <- schema_from_column_metadata(
          meta = metadata$columns[[col_name]],
          schema = narrow::as_narrow_schema(x$schema[[col_name]])
        )
      }

      result <- wk_handle_wrapper(
        narrow::as_narrow_array_stream(array_or_chunked_array),
        handler,
        geoarrow_schema = geoarrow_schema,
        geoarrow_n_features = array_or_chunked_array$length()
      )

      if (!is.null(result)) {
        wk::wk_crs(result) <- recursive_extract_narrow_schema(geoarrow_schema, "crs")
        geodesic <- recursive_extract_narrow_schema(geoarrow_schema, "crs")
        if (identical(geodesic, "true")) {
          wk::wk_is_geodesic(result) <- TRUE
        }
      }

      result
    }
  )

  names(handleable_results) <- handleable_cols
  result_null <- vapply(handleable_results, is.null, logical(1))
  handleable_results <- handleable_results[!result_null]

  attr_cols <- setdiff(names(x), handleable_cols)

  if (length(attr_cols) > 0) {
    attr_results <- as.data.frame(x[attr_cols])
    attr_results[names(handleable_results)] <- handleable_results
    attr_results
  } else if (length(handleable_results) > 0) {
    new_data_frame(handleable_results)
  } else {
    new_data_frame(list(), nrow = 1)
  }
}

#' @rdname read_geoarrow_parquet
#' @export
geoarrow_collect.RecordBatch <- function(x, ..., handler = NULL, metadata = NULL) {
  geoarrow_collect.Table(x, ..., handler = handler, metadata = metadata)
}


#' @rdname read_geoarrow_parquet
#' @export
geoarrow_collect.RecordBatchReader <- function(x, trans = identity, ..., handler = NULL,
                                               metadata = NULL) {
  batches <- vector("list", 1024)
  i <- 0L
  while (!is.null(batch <- x$read_next_batch())) {
    i <- i + 1L
    batch_df <- geoarrow_collect.RecordBatch(
      batch, ...,
      handler = handler,
      metadata = metadata
    )

    batches[[i]] <- trans(batch_df)
  }

  if (i < length(batches)) {
    batches <- batches[seq_len(i)]
  }

  dplyr::bind_rows(!!! batches)
}

#' @rdname read_geoarrow_parquet
#' @export
geoarrow_collect.Dataset <- function(x, trans = identity, ..., handler = NULL,
                                     metadata = NULL) {
  metadata <- geoarrow_object_metadata(x, metadata)
  reader <- arrow::Scanner$create(x, ...)$ToRecordBatchReader()
  geoarrow_collect(reader, trans, handler = handler, metadata = metadata)
}


#' @rdname read_geoarrow_parquet
#' @export
geoarrow_collect.arrow_dplyr_query <- function(x, ..., handler = NULL, metadata = NULL) {
  table <- dplyr::collect(x, as_data_frame = FALSE)
  geoarrow_collect(table, ..., handler = handler, metadata = metadata)
}

wk_handle_wrapper <- function(handleable, handler, ...) {
  if (is.null(handler)) {
    # for now...we should use a geoarrow_vctr for this but we need
    # Concatenate for that to work (or for narrow_vctr to support
    # chunked arrays or streams)
    handler <- wk::wkb_writer()
  }

  wk::wk_handle(handleable, handler, ...)
}

geoarrow_object_metadata <- function(x, metadata = NULL) {
  if (is.list(metadata) || is.null(metadata)) {
    as.list(metadata)
  } else {
    jsonlite::fromJSON(metadata)
  }
}

guess_metadata_columns <- function(x) {
  # first, look for extension metadata
  guessed_encodings <- vapply(names(x), function(col_name) {
    schema <- narrow::as_narrow_schema(x$schema[[col_name]])
    ext <- schema$metadata[["ARROW:extension:name"]] %||% ""
    if (grepl("^geoarrow\\.", ext)) "::embedded::" else NA_character_
  }, character(1))
  names(guessed_encodings) <- names(x)
  guessed_encodings <- guessed_encodings[!is.na(guessed_encodings)]

  if (length(guessed_encodings) > 0) {
    return(lapply(guessed_encodings, function(e) list(encoding = e)))
  }

  # then, try guessing
  guessed_encodings <- vapply(names(x), function(col_name) {
    tryCatch(
      guess_column_encoding(x$schema[[col_name]]),
      error = function(e) NA_character_
    )
  }, character(1))
  names(guessed_encodings) <- names(x)
  guessed_encodings <- guessed_encodings[!is.na(guessed_encodings)]

  # exclude WKT and WKB guesses because these are possibly generic column
  # types and would be annoying if guessed wrong
  guessed_encodings <- guessed_encodings[!(guessed_encodings %in% c("WKT", "WKB"))]

  lapply(guessed_encodings, function(e) list(encoding = e))
}
