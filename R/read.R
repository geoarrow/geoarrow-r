
#' Write geometry as Apache Parquet files
#'
#' @param file A file or InputStream to read; passed to
#'   [arrow::read_parquet()].
#' @param as.data.frame Use `FALSE` to return an [arrow::Table]
#'   instead of a dataa.frame.
#' @param handler A [wk handler][wk::wk_handle] to use when `as.data.frame`
#'   is TRUE for all geometry columns.
#' @param metadata Optional metadata to include to override metadata available
#'   in the file.
#' @inheritDotParams arrow::write_parquet
#'
#' @return The result of [arrow::read_parquet()], with geometry
#'
#' @export
#'
geoarrow_read_parquet <- function(file, ..., as.data.frame = TRUE,
                                  handler = wk::sfc_writer(),
                                  metadata = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for read_geoarrow_parquet()", call. = FALSE) # nocov
  }


}
