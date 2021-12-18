
#' Write geometry as Apache Parquet files
#'
#' @inheritDotParams arrow::write_parquet
#' @inheritParams geoarrow_create
#'
#' @return The result of [arrow::read_parquet()], with geometry
#'
#' @export
#'
read_geoarrow_parquet <- function(file, ..., writer = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for read_geoarrow_parquet()", call. = FALSE) # nocov
  }


}
