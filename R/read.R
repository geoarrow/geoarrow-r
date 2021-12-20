
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
#' @inheritDotParams arrow::write_parquet
#'
#' @return The result of [arrow::read_parquet()], with geometry
#'   columns processed according to `handler`.
#'
#' @export
#'
read_geoarrow_parquet <- function(file, ..., col_select = NULL,
                                  as_data_frame = TRUE,
                                  handler = wk::sfc_writer,
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
    # we could avoid reading a copy here by making the handlers
    # accept an ArrowArrayStream with an overridden schema
    handleable_table <- arrow::read_parquet(
      file,
      ...,
      col_select = !! handleable_cols,
      as_data_frame = FALSE
    )

    handleable_results <- lapply(
      handleable_cols,
      function(col_name) {
        chunked <- handleable_table[[col_name]]
        geoarrow_schema <- schema_from_column_metadata(
          meta = metadata$columns[[col_name]],
          schema = carrow::as_carrow_schema(chunked$type)
        )
        result <- vector("list", chunked$num_chunks)
        for (i in seq_along(result)) {
          array <- carrow::carrow_array(
            geoarrow_schema,
            carrow::as_carrow_array(chunked$chunk(i - 1L))$array_data,
            validate = FALSE
          )
          result[[i]] <- wk::wk_handle(array, wk::as_wk_handler(handler))
        }

        do.call(c, result)
      }
    )

    names(handleable_results) <- handleable_cols
    result_null <- vapply(handleable_results, is.null, logical(1))
    handleable_results <- handleable_results[!result_null]
    handleable_df <- new_data_frame(handleable_results, nrow = handleable_table$num_rows)
  } else {
    handleable_df <- NULL
  }

  tbl <- dplyr::bind_cols(attrs_df, handleable_df)
  tbl[intersect(col_select, names(tbl))]
}
