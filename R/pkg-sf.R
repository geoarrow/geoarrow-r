
#' @rdname read_geoparquet
#' @export
read_geoparquet_sf <- function(file, ...) {
  sf::st_as_sf(read_geoparquet(file, ..., handler = wk::sfc_writer))
}

#' @rdname read_geoparquet
#' @export
geoarrow_collect_sf <- function(x, ..., metadata = NULL) {
  sf::st_as_sf(
    geoarrow_collect(x, ..., handler = wk::sfc_writer, metadata = metadata)
  )
}

# exported in zzz.R
as_arrow_table.sf <- function(x, ..., schema = NULL) {
  # not supported for now
  stopifnot(is.null(schema))

  # add geoparquet metadata because the most likely destination is
  # write_parquet() or write_feather()
  as_geoarrow_table(x, geoparquet_metadata = TRUE)
}