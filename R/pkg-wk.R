
#' @importFrom wk wk_crs
#' @export
wk_crs.narrow_array <- function(x) {
  recursive_extract_narrow_schema(x$schema, "crs")
}

#' @importFrom wk wk_is_geodesic
#' @export
wk_is_geodesic.narrow_array <- function(x) {
  identical(
    recursive_extract_narrow_schema(x$schema, "edges"),
    "spherical"
  )
}

#' @importFrom wk wk_set_crs
#' @export
wk_set_crs.narrow_array <- function(x, crs) {
  crs <- if (!is.null(crs)) wk::wk_crs_proj_definition(crs, verbose = TRUE)
  x$schema <- geoarrow_schema_set_crs(x$schema, crs)
  x
}

#' @importFrom wk wk_set_geodesic
#' @export
wk_set_geodesic.narrow_array <- function(x, geodesic) {
  x$schema <- geoarrow_schema_set_geodesic(x$schema, geodesic)
  x
}


#' @export
wk_crs.geoarrow_vctr <- function(x) {
  recursive_extract_narrow_schema(attr(x, "schema", exact = TRUE), "crs")
}

#' @export
wk_is_geodesic.geoarrow_vctr <- function(x) {
  identical(
    recursive_extract_narrow_schema(attr(x, "schema", exact = TRUE), "edges"),
    "spherical"
  )
}

#' @export
wk_set_crs.geoarrow_vctr <- function(x, crs) {
  crs <- if (!is.null(crs)) wk::wk_crs_proj_definition(crs, verbose = TRUE)
  attr(x, "schema") <- geoarrow_schema_set_crs(attr(x, "schema", exact = TRUE), crs)
  x
}

#' @export
wk_set_geodesic.geoarrow_vctr <- function(x, geodesic) {
  attr(x, "schema") <- geoarrow_schema_set_geodesic(
    attr(x, "schema", exact = TRUE),
    geodesic
  )

  x
}


geoarrow_schema_set_crs <- function(schema, crs) {
  recursive_modify_narrow_schema(
    schema,
    crs = crs,
    extensions = c(
      "geoarrow.point",
      "geoarrow.wkt",
      "geoarrow.wkb"
    )
  )
}

geoarrow_schema_set_geodesic <- function(schema, geodesic) {
  edges <- if (isTRUE(geodesic)) "spherical" else NULL

  recursive_modify_narrow_schema(
    schema,
    edges = edges,
    extensions = c(
      "geoarrow.linestring",
      "geoarrow.polygon",
      "geoarrow.wkt",
      "geoarrow.wkb"
    )
  )
}

recursive_extract_narrow_schema <- function(schema, key) {
  geo_metadata <- geoarrow_metadata(schema)
  if (!is.null(geo_metadata[[key]])) {
    return(geo_metadata[[key]])
  } else {
    for (child in schema$children) {
      geo_metadata <- geoarrow_metadata(child)
      if (!is.null(geo_metadata[[key]])) {
        return(geo_metadata[[key]])
      } else {
        return(recursive_extract_narrow_schema(child, key))
      }
    }
  }

  NULL
}

recursive_modify_narrow_schema <- function(schema, ..., extensions) {
  ext <- schema$metadata[["ARROW:extension:name"]] %||% NA_character_
  if (ext %in% extensions) {
    do.call(geoarrow_set_metadata, list(schema, ...))
  } else {
    for (i in seq_along(schema$children)) {
      schema$children[[i]] <- recursive_modify_narrow_schema(
        schema$children[[i]],
        ...,
        extensions = extensions
      )
    }

    schema
  }
}
