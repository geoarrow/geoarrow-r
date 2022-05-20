
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

st_as_sf.ArrowTabular <- function(x, ...) {
  geoarrow_collect_sf(x, ...)
}

st_geometry.ArrowTabular <- function(x, ...) {
  schema <- x$.data$schema %||% x$schema
  for (i in seq_len(schema$num_fields)) {
    if (inherits(schema$field(i - 1)$type, "GeoArrowType")) {
      name <- schema$field(i - 1)$name
      return(geoarrow_collect_sf(dplyr::select(x, !! name))[[name]])
    }
  }

  stop("No geometry column present")
}

st_crs.ArrowTabular <- function(x, ...) {
  schema <- x$.data$schema %||% x$schema
  for (i in seq_len(schema$num_fields)) {
    if (inherits(schema$field(i - 1)$type, "GeoArrowType")) {
      return(sf::st_crs(schema$field(i - 1)$type$crs))
    }
  }

  stop("No geometry column present")
}

st_bbox.ArrowTabular <- function(x) {
  schema <- x$.data$schema %||% x$schema
  for (i in seq_len(schema$num_fields)) {
    if (inherits(schema$field(i - 1)$type, "GeoArrowType")) {
      name <- schema$field(i - 1)$name
      x_geom <- dplyr::select(x, !! name)
      rbr <- arrow::as_record_batch_reader(x_geom)
      geom_stream <- narrow::narrow_array_stream_function(
        rbr$schema[[1]]$type,
        function() {
          batch <- rbr$read_next_batch()
          if (is.null(batch)) {
            NULL
          } else {
            batch[[1]]
          }
        }
      )

      bbox <- wk::wk_handle(geom_stream, wk::wk_bbox_handler())
      wk::wk_crs(bbox) <- st_crs.ArrowTabular(x)
      return(sf::st_bbox(bbox))
    }
  }

  stop("No geometry column present")
}

st_as_sf.geoarrow_vctr <- function(x, ...) {
  sf::st_as_sf(new_data_frame(list(geometry = sf::st_as_sfc(x, ...))))
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
