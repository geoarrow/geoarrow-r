
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

  # Add geoparquet metadata because the most likely destination is
  # write_parquet() or write_feather(). Strip the sf class, though,
  # because arrow may run into errors when attempting to restore the
  # data frame attributes.
  x <- tibble::as_tibble(x)

  as_geoarrow_table(x, geoparquet_metadata = TRUE)
}

st_as_sfc.geoarrow_vctr <- function(x, ...) {
  sf::st_set_crs(
    wk::wk_handle(x, wk::sfc_writer()),
    sf::st_crs(wk::wk_crs(x))
  )
}

st_geometry.geoarrow_vctr <- function(x, ...) {
  st_as_sfc.geoarrow_vctr(x)
}

st_crs.geoarrow_vctr <- function(x, ...) {
  sf::st_crs(wk::wk_crs(x))
}

st_bbox.geoarrow_vctr <- function(x) {
  sf::st_bbox(wk::wk_bbox(x))
}
