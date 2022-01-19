
#' Write geometry as Apache Parquet files
#'
#' @param file A file or InputStream to read; passed to
#'   [arrow::read_parquet()].
#' @param as_data_frame Use `FALSE` to return an [arrow::Table]
#'   instead of a data.frame.
#' @param col_select A character vector of columns to include.
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
read_geoarrow_parquet <- function(file, ..., col_select = NULL,
                                  as_data_frame = TRUE,
                                  handler = NULL,
                                  metadata = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for read_geoarrow_parquet()", call. = FALSE) # nocov
  }

  if (!as_data_frame) {
    return(arrow::read_parquet(file, ..., col_select = {{ col_select }}, as_data_frame = FALSE))
  }

  # use dataset to get column names and metadata
  ds <- arrow::open_dataset(file, partitioning = NULL)
  metadata <- metadata %||% jsonlite::fromJSON(ds$metadata$geo)

  # we can't use fancy dplyr selections with col_select
  stopifnot(is.null(col_select) || is.character(col_select))

  col_names <- names(ds$schema)
  col_select <- col_select %||% col_names
  handleable_cols <- intersect(col_select, names(metadata$columns))
  attr_cols <- intersect(col_select, setdiff(col_names, handleable_cols))

  if (length(attr_cols) > 0) {
    attrs_df <- arrow::read_parquet(file, ..., col_select = !! attr_cols)
  } else {
    attrs_df <- NULL
  }

  if (length(handleable_cols) > 0) {
    # currently read everything into memory once and then do the conversion
    handleable_table <- arrow::read_parquet(
      file,
      ...,
      col_select = !! handleable_cols,
      as_data_frame = FALSE
    )

    handleable_df <- geoarrow_collect(
      handleable_table,
      handler = handler,
      metadata = metadata
    )
  } else {
    handleable_df <- NULL
  }

  tbl <- dplyr::bind_cols(attrs_df, handleable_df)
  tbl[intersect(col_select, names(tbl))]
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

  handleable_cols <- intersect(x$ColumnNames(), names(metadata$columns))

  if (length(handleable_cols) == 0) {
    return(as.data.frame(x))
  }

  if (is.null(handler)) {
    handler <- collect_default_handler()
  }

  handleable_results <- lapply(
    handleable_cols,
    function(col_name) {
      array_or_chunked_array <- x[[col_name]]
      arrow_type <- array_or_chunked_array$type

      geoarrow_schema <- schema_from_column_metadata(
        meta = metadata$columns[[col_name]],
        schema = carrow::as_carrow_schema(arrow_type)
      )

      result <- wk::wk_handle(
        carrow::as_carrow_array_stream(array_or_chunked_array),
        wk::as_wk_handler(handler),
        geoarrow_schema = geoarrow_schema,
        geoarrow_n_features = array_or_chunked_array$length()
      )

      if (!is.null(result)) {
        wk::wk_set_crs(result, wk_crs_carrow_schema(geoarrow_schema))
      } else {
        result
      }
    }
  )

  names(handleable_results) <- handleable_cols
  result_null <- vapply(handleable_results, is.null, logical(1))
  handleable_results <- handleable_results[!result_null]

  attr_cols <- setdiff(x$ColumnNames(), handleable_cols)

  if (length(attr_cols) > 0) {
    attr_results <- x[attr_cols]
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
    batch_df <- geoarrow_collect.Table(
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

collect_default_handler <- function() {
  wk::sfc_writer
}

geoarrow_object_metadata <- function(x, metadata = NULL) {
  metadata <- metadata %||% x$metadata$geo
  if (is.null(metadata)) {
    stop(
      "`x$metadata$geo` was not found and `metadata` was not specified",
      call. = FALSE
    )
  }

  if (is.list(metadata)) {
    metadata
  } else {
    jsonlite::fromJSON(metadata)
  }
}
