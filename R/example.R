
#' Create example geometry objects
#'
#' @param which An example name. Valid example names are
#'   - "nc"
#'   - "point", "linestring", "polygon", "multipoint",
#'     "multilinestring", "multipolygon", "geometrycollection"
#'   - One of the above with the "_z", "_m", or "_zm" prefix.
#' @inheritParams geoarrow_create
#' @inheritParams geoarrow_schema_point
#'
#' @return The result of [geoarrow_create()] with the specified example type.
#' @export
#'
#' @examples
#' geoarrow_example("polygon")
#'
geoarrow_example <- function(which = "nc", schema = NULL,
                             strict = FALSE,
                             crs = NULL,
                             edges = "planar") {
  as_geoarrow_vctr(
    geoarrow_example_narrow_array(
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
                                   crs = NULL,
                                   edges = "planar") {
  narrow::from_narrow_array(
    geoarrow_example_narrow_array(
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
geoarrow_example_narrow_array <- function(which = "nc", schema = NULL,
                                          strict = FALSE,
                                          crs = NULL,
                                          edges = "planar") {
  all_examples <- c(
    "nc",
    names(geoarrow_example_wkt_raw),
    paste0(names(geoarrow_example_wkt_raw), "_z"),
    paste0(names(geoarrow_example_wkt_raw), "_m"),
    paste0(names(geoarrow_example_wkt_raw), "_zm")
  )

  which <- match.arg(which, names(geoarrow_example_wkt_raw))
  edges <- match.arg(edges, c("planar", "spherical"))
  geodesic <- identical(edges, "spherical")

  if (identical(which, "nc")) {
    stopifnot(is.null(crs))
    stop("Not implemented")
  } else {
    base_name <- gsub("_[zm]+$", "", which)
    handleable <- wk::wkt(geoarrow_example_wkt_raw[[base_name]])

    if (endsWith(which, "_z") || endsWith(which, "_zm")) {
      handleable <- wk::wk_set_z(100)
    }

    if (endsWith(which, "_m") || endsWith(which, "_zm")) {
      handleable <- wk::wk_set_m(200)
    }

    wk::wk_crs(handleable) <- crs
  }

  wk::wk_is_geodesic(handleable) <- geodesic

  if (is.null(schema)) {
    schema <- geoarrow_schema_default(handleable)
  }

  geoarrow_create(handleable, schema = schema, strict = strict)
}

geoarrow_example_wkt_raw <- list(
  "point" = c("POINT (30 10)", "POINT EMPTY", NA),
  "linestring" = c(
    "LINESTRING (30 10, 10 30, 40 40)",
    "LINESTRING EMPTY",
    NA
  ),
  "polygon" = c(
    "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),
              (20 30, 35 35, 30 20, 20 30))",
    "POLYGON EMPTY",
    NA
  ),
  "multipoint" = c(
    "MULTIPOINT (30 10)",
    "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
    "MULTIPOINT (10 40, 40 30, 20 20, 30 10)",
    "MULTIPOINT EMPTY",
    NA
  ),
  "multilinestring" = c(
    "MULTILINESTRING ((30 10, 10 30, 40 40))",
    "MULTILINESTRING ((10 10, 20 20, 10 40),
                      (40 40, 30 30, 40 20, 30 10))",
    "MULTILINESTRING EMPTY",
    NA
  ),
  "multipolygon" = c(
    "MULTIPOLYGON (((30 10, 40 40, 20 40, 10 20, 30 10)))",
    "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),
                   ((15 5, 40 10, 10 20, 5 10, 15 5)))",
    "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),
                   ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),
                    (30 20, 20 15, 20 25, 30 20)))",
    "MULTIPOLYGON EMPTY",
    NA
  ),
  "geometrycollection" = c(
    "GEOMETRYCOLLECTION (POINT (30 10))",
    "GEOMETRYCOLLECTION (LINESTRING (30 10, 10 30, 40 40))",
    "GEOMETRYCOLLECTION (POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)))",
    "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (30 10)))",
    "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (LINESTRING (30 10, 10 30, 40 40)))",
    "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (
       POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))))",
    "GEOMETRYCOLLECTION(
       POINT (30 10),
       LINESTRING (30 10, 10 30, 40 40),
       POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))))",
    "GEOMETRYCOLLECTION EMPTY",
    NA
  )
)
