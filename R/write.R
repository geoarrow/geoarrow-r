
#' Write 'GeoParquet' files
#'
#' Whereas [arrow::write_parquet()] will happily convert/write geometry columns
#' to Parquet format when the geoarrow package is loaded, `write_geoparquet()`
#' generates additional file-level metadata and chooses a more generic encoding
#' to improve the interoperability of the Parquet file when read by non-Arrow
#' Parquet readers.
#'
#' @inheritDotParams arrow::write_parquet
#' @inheritParams geoarrow_create_narrow
#'
#' @return The result of [arrow::write_parquet()], invisibly
#' @export
#'
#' @examples
#' tf <- tempfile()
#' write_geoparquet(data.frame(col1 = 1:5, col2 = wk::xy(1:5, 6:10)), tf)
#' read_geoparquet(tf)
#' unlink(tf)
#'
write_geoparquet <- function(handleable, ..., schema = NULL, strict = FALSE) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for write_geoparquet()", call. = FALSE) # nocov
  }

  table <- as_geoarrow_table(
    handleable,
    schema,
    strict,
    # workaround because Arrow Parquet can't roundtrip null fixed-width list
    # elements (https://issues.apache.org/jira/browse/ARROW-8228)
    null_point_as_empty = is.null(schema) ||
      startsWith(schema$format, "+w:"),
    geoparquet_metadata = TRUE
  )

  # write!
  arrow::write_parquet(table, ...)
}


#' Create Arrow Tables
#'
#' Like [arrow::as_arrow_table()], `as_geoarrow_table()` creates an
#' [arrow::Table] object. The geo-enabled version has a few additional options
#' that customize the specific output schema and table-level metadata
#' that is generated.
#'
#' @inheritParams write_geoparquet
#' @inheritParams geoarrow_create_narrow
#' @param geoparquet_metadata Use `TRUE` to add GeoParquet metadata to the
#'   output schema metadata.
#'
#' @return an [arrow::Table].
#' @export
#'
#' @examples
#' as_geoarrow_table(data.frame(col1 = 1:5, col2 = wk::xy(1:5, 6:10)))
#'
as_geoarrow_table <- function(handleable, schema = NULL, strict = FALSE,
                              null_point_as_empty = FALSE,
                              geoparquet_metadata = FALSE) {
  if (!is.data.frame(handleable)) {
    handleable <- data.frame(geometry = handleable)
  } else {
    handleable <- as.data.frame(handleable)
  }

  is_handleable <- vapply(handleable, is_handleable_column, logical(1))
  df_attr <- handleable[!is_handleable]
  df_handleable <- handleable[is_handleable]

  if (is.null(schema)) {
    arrays_handleable <- lapply(
      df_handleable,
      geoarrow_create_narrow,
      null_point_as_empty = null_point_as_empty
    )
  } else {
    arrays_handleable <- lapply(
      df_handleable,
      geoarrow_create_narrow,
      schema = schema,
      strict = strict,
      null_point_as_empty = null_point_as_empty
    )
  }

  # ship to arrow
  vctr_handleable <- lapply(arrays_handleable, narrow::narrow_vctr)
  arrays <- c(unclass(df_attr), vctr_handleable)[colnames(handleable)]
  batch <- arrow::record_batch(!!! arrays)

  # create file metadata
  if (geoparquet_metadata) {
    handleable_schema <- narrow::narrow_schema(
      format = "+s",
      children = lapply(arrays_handleable, "[[", "schema")
    )

    for (i in seq_along(handleable_schema$children)) {
      handleable_schema$children[[i]]$name <- names(arrays_handleable)[i]
    }

    file_metadata <- geoparquet_metadata(handleable_schema, arrays = arrays_handleable)

    batch$metadata$geo <- jsonlite::toJSON(
      file_metadata,
      null = "null",
      auto_unbox = TRUE,
      always_decimal = TRUE
    )
  }

  arrow::as_arrow_table(batch)
}

is_handleable_column <- function(x) {
  tryCatch({wk::wk_vector_meta(x); TRUE}, error = function(e) FALSE)
}
