
#' Create example geometry objects
#'
#' @param which An example name. Valid example names are
#'   - "nc"
#'   - "point", "linestring", "polygon", "multipoint",
#'     "multilinestring", "multipolygon", "geometrycollection"
#'   - One of the above with the "_z", "_m", or "_zm" suffix.
#' @inheritParams geoarrow_create_narrow
#' @inheritParams geoarrow_schema_point
#'
#' @return The result of [geoarrow_create_narrow_from_buffers()] with the specified example type.
#' @export
#'
#' @examples
#' geoarrow_example("polygon")
#'
geoarrow_example <- function(which = "nc", schema = NULL,
                             strict = FALSE,
                             crs = NA,
                             edges = "planar") {
  geoarrow_create(
    geoarrow_example_narrow(
      which = which,
      schema = schema,
      crs = crs,
      edges = edges
    )
  )
}

#' @rdname geoarrow_example
#' @export
geoarrow_example_Array <- function(which = "nc", schema = NULL,
                                   strict = FALSE,
                                   crs = NA,
                                   edges = "planar") {
  narrow::from_narrow_array(
    geoarrow_example_narrow(
      which = which,
      schema = schema,
      crs = crs,
      edges = edges
    ),
    arrow::Array
  )
}

#' @rdname geoarrow_example
#' @export
geoarrow_example_narrow <- function(which = "nc", schema = NULL,
                                    strict = FALSE,
                                    crs = NA,
                                    edges = "planar") {
  all_examples <- geoarrow::geoarrow_example_wkt
  match.arg(which, names(all_examples))

  match.arg(edges, c("planar", "spherical"))
  geodesic <- identical(edges, "spherical")

  handleable <- all_examples[[which]]

  if (!identical(crs, NA)) {
    wk::wk_crs(handleable) <- crs
  }

  wk::wk_is_geodesic(handleable) <- geodesic

  if (is.null(schema)) {
    schema <- geoarrow_schema_default(handleable)
  }

  geoarrow_create_narrow_from_buffers(handleable, schema = schema, strict = strict)
}

#' @rdname geoarrow_example
"geoarrow_example_wkt"
