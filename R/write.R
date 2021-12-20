
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

  if (!is.data.frame(handleable)) {
    handleable <- data.frame(geometry = handleable)
  } else {
    handleable <- as.data.frame(handleable)
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

  # create file metadata
  handleable_schema <- carrow::carrow_schema(
    format = "+s",
    children = lapply(arrays_handleable, "[[", "schema")
  )
  for (i in seq_along(handleable_schema$children)) {
    handleable_schema$children[[i]]$name <- names(arrays_handleable)[i]
  }
  file_metadata <- geoarrow_metadata_table(handleable_schema)

  # create arrow Arrays
  arrays_attr <- lapply(df_attr, arrow::Array$create)
  arrays_handleable_arrow <- lapply(arrays_handleable, carrow::from_carrow_array, arrow::Array)
  arrays <- c(arrays_attr, arrays_handleable_arrow)[names(handleable)]
  batch <- arrow::record_batch(!!! arrays)

  # add file metadata
  batch$metadata$geo <- jsonlite::toJSON(file_metadata, null = "null", auto_unbox = TRUE)

  # write!
  arrow::write_parquet(batch, ...)
}

is_handleable_column <- function(x) {
  tryCatch({wk::wk_vector_meta(x); TRUE}, error = function(e) FALSE)
}
