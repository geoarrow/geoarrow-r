
#' @importFrom wk wk_crs
#' @export
wk_crs.narrow_array <- function(x) {
  recursive_extract_narrow_schema(x$schema, "crs")
}

#' @importFrom wk wk_is_geodesic
#' @export
wk_is_geodesic.narrow_array <- function(x) {
  identical(
    recursive_extract_narrow_schema(x$schema, "geodesic"),
    "true"
  )
}

#' @importFrom wk wk_set_crs
#' @export
wk_set_crs.narrow_array <- function(x, crs) {
  crs <- if (!is.null(crs)) wk::wk_crs_proj_definition(crs, verbose = TRUE)
  x$schema <- recursive_modify_narrow_schema(
    x$schema,
    crs = crs,
    extensions = c(
      "geoarrow.point",
      "geoarrow.wkt",
      "geoarrow.wkb",
      "geoarrow.gejson"
    )
  )

  x
}

#' @importFrom wk wk_set_geodesic
#' @export
wk_set_geodesic.narrow_array <- function(x, geodesic) {
  x$schema <- recursive_modify_narrow_schema(
    x$schema,
    geodesic = geodesic,
    extensions = c(
      "geoarrow.linestring",
      "geoarrow.polygon",
      "geoarrow.wkt",
      "geoarrow.wkb"
    )
  )

  x
}


#' @export
wk_crs.narrow_vctr_geoarrow <- function(x) {
  wk_crs(narrow::as_narrow_array(x))
}

#' @export
wk_is_geodesic.narrow_vctr_geoarrow <- function(x) {
  wk_is_geodesic(narrow::as_narrow_array(x))
}

#' @export
wk_set_crs.narrow_vctr_geoarrow <- function(x, crs) {
  array <- attr(x, "array", exact = TRUE)
  attr(x, "array") <- wk::wk_set_crs(array, crs)
  x
}

#' @export
wk_set_geodesic.narrow_vctr_geoarrow <- function(x, geodesic) {
  array <- attr(x, "array", exact = TRUE)
  attr(x, "array") <- wk::wk_set_geodesic(array, geodesic)
  x
}

recursive_extract_narrow_schema <- function(schema, key) {
  geo_metadata <- geoarrow_metadata(schema)
  if (!is.null(geo_metadata[[key]])) {
    geo_metadata[[key]]
  } else {
    for (child in schema$children) {
      geo_metadata <- geoarrow_metadata(child)
      if (!is.null(geo_metadata[[key]])) {
        return(geo_metadata[[key]])
      } else {
        return(recursive_extract_narrow_schema(child))
      }
    }
  }
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
