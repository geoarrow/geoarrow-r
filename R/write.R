
#' Write geometry as Apache Parquet files
#'
#' @inheritDotParams arrow::write_parquet
#' @inheritParams geoarrow_create
#'
#' @return The result of [arrow::write_parquet()], invisibly
#' @export
#'
write_geoarrow_parquet <- function(handleable, ..., schema = NULL, strict = FALSE) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for write_geoarrow_parquet()", call. = FALSE) # nocov
  }

  batch <- geoarrow_create_record_batch(handleable, schema, strict)
  arrow::write_parquet(batch, ...)
}

geoarrow_create_record_batch <- function(handleable, schema = NULL, strict = FALSE) {
  if (!is.data.frame(handleable)) {
    handleable <- data.frame(geometry = handleable)
  }

  is_handleable <- vapply(handleable, is_handleable_column, logical(1))
  df_attr <- handleable[!is_handleable]
  df_handleable <- handleable[is_handleable]

  if (is.null(schema)) {
    arrays_handleable <- lapply(df_handleable, geoarrow_create)
  } else {
    arrays_handleable <- lapply(
      df_handleable,
      geoarrow_create,
      schema = schema,
      strict = strict
    )
  }

  arrays_attr <- lapply(df_attr, arrow::Array$create)
  arrays_handleable_arrow <- lapply(arrays_handleable, carrow::from_carrow_array, arrow::Array)
  arrays <- c(arrays_attr, arrays_handleable_arrow)[names(handleable)]

  arrow::record_batch(!!! arrays)
}

is_handleable_column <- function(x) {
  tryCatch({wk::wk_vector_meta(x); TRUE}, error = function(e) FALSE)
}
