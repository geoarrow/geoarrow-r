
#' @importFrom wk wk_handle
#' @export
wk_handle.geoarrow_vctr <- function(handleable, handler, ...) {
  geoarrow_handle(handleable, handler, size = length(handleable))
}

#' @importFrom wk wk_crs
#' @export
wk_crs.geoarrow_vctr <- function(x) {
  parsed <- geoarrow_schema_parse(attr(x, "schema", exact = TRUE))
  if (parsed$crs_type == enum$CrsType$NONE) {
    NULL
  } else {
    parsed$crs
  }
}

#' @importFrom wk wk_is_geodesic
#' @export
wk_is_geodesic.geoarrow_vctr <- function(x) {
  parsed <- geoarrow_schema_parse(attr(x, "schema", exact = TRUE))
  parsed$edge_type == enum$EdgeType$SPHERICAL
}

#' @export
as_geoarrow_array.wk_wkt <- function(x, ..., schema = NULL) {
  if (!is.null(schema)) {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
    if (!identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkt")) {
      return(NextMethod())
    }
  } else {
    schema <- nanoarrow::infer_nanoarrow_schema(x)
  }

  schema_storage <- nanoarrow::nanoarrow_schema_modify(
    schema,
    list(metadata = NULL)
  )
  storage <- nanoarrow::as_nanoarrow_array(unclass(x), schema = schema_storage)
  storage_shallow_copy <- nanoarrow::nanoarrow_allocate_array()
  nanoarrow::nanoarrow_pointer_export(storage, storage_shallow_copy)
  nanoarrow::nanoarrow_array_set_schema(storage_shallow_copy, schema)

  storage_shallow_copy
}

#' @export
as_geoarrow_array.wk_wkb <- function(x, ..., schema = NULL) {
  if (!is.null(schema)) {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
    if (!identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.wkb")) {
      return(NextMethod())
    }
  } else {
    schema <- nanoarrow::infer_nanoarrow_schema(x)
  }

  schema_storage <- nanoarrow::nanoarrow_schema_modify(
    schema,
    list(metadata = NULL)
  )
  storage <- nanoarrow::as_nanoarrow_array(unclass(x), schema = schema_storage)
  storage_shallow_copy <- nanoarrow::nanoarrow_allocate_array()
  nanoarrow::nanoarrow_pointer_export(storage, storage_shallow_copy)
  nanoarrow::nanoarrow_array_set_schema(storage_shallow_copy, schema)

  storage_shallow_copy
}

#' @export
as_geoarrow_array.wk_xy <- function(x, ..., schema = NULL) {
  if (!is.null(schema)) {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
    if (!identical(schema$metadata[["ARROW:extension:name"]], "geoarrow.point")) {
      return(NextMethod())
    }
  }

  schema <- nanoarrow::infer_nanoarrow_schema(x)
  data <- unclass(x)
  geoarrow_array_from_buffers(
    schema,
    # Treating NULLs as EMPTY for now
    c(list(NULL), data)
  )
}

#' @importFrom nanoarrow infer_nanoarrow_schema
#' @export
infer_nanoarrow_schema.wk_wkt <- function(x, ...) {
  data <- unclass(x)
  schema <- nanoarrow::infer_nanoarrow_schema(data)
  schema_for_metadata <- wk_geoarrow_schema(x, na_extension_wkt)
  schema$metadata <- schema_for_metadata$metadata
  schema
}

#' @importFrom nanoarrow infer_nanoarrow_schema
#' @export
infer_nanoarrow_schema.wk_wkb <- function(x, ...) {
  data <- structure(x, class = "blob")
  schema <- nanoarrow::infer_nanoarrow_schema(data)
  schema_for_metadata <- wk_geoarrow_schema(x, na_extension_wkb)
  schema$metadata <- schema_for_metadata$metadata
  schema
}

#' @importFrom nanoarrow infer_nanoarrow_schema
#' @export
infer_nanoarrow_schema.wk_xy <- function(x, ...) {
  data <- unclass(x)
  dims <- paste0(toupper(names(data)), collapse = "")
  wk_geoarrow_schema(x, na_extension_geoarrow, "POINT", dimensions = dims)
}

#' @export
convert_array.wk_wkt <- function(array, to, ...) {
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  geo <- geoarrow_schema_parse(schema)
  vctr <- as_geoarrow_vctr(array)

  if (geo$extension_name == "geoarrow.wkt") {
    out <- wk::new_wk_wkt(nanoarrow::convert_array(force_array_storage(array)))
  } else {
    out <- wk::wk_handle(vctr, wk::wkt_writer())
  }

  wk::wk_crs(out) <- wk::wk_crs_output(vctr, to)
  wk::wk_is_geodesic_output(vctr, to)
  wk::wk_is_geodesic(out) <- wk::wk_is_geodesic_output(vctr, to)
  out
}

#' @export
convert_array.wk_wkb <- function(array, to, ...) {
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  geo <- geoarrow_schema_parse(schema)
  vctr <- as_geoarrow_vctr(array)

  if (geo$extension_name == "geoarrow.wkb") {
    storage <- nanoarrow::convert_array(force_array_storage(array))
    # Comes back as a blob::blob
    attributes(storage) <- NULL
    out <- wk::new_wk_wkb(storage)
  } else {
    out <- wk::wk_handle(vctr, wk::wkb_writer())
  }

  wk::wk_crs(out) <- wk::wk_crs_output(vctr, to)
  wk::wk_is_geodesic(out) <- wk::wk_is_geodesic_output(vctr, to)
  out
}

#' @export
convert_array.wk_xy <- function(array, to, ...) {
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  geo <- geoarrow_schema_parse(schema)
  vctr <- as_geoarrow_vctr(array)

  if (geo$extension_name == "geoarrow.point") {
    out <- wk::as_xy(nanoarrow::convert_array(force_array_storage(array)))
  } else {
    out <- wk::wk_handle(vctr, wk::xy_writer())
  }

  wk::wk_crs(out) <- wk::wk_crs_output(vctr, to)
  wk::wk_is_geodesic(out) <- wk::wk_is_geodesic_output(vctr, to)
  wk::as_xy(out, dims = names(unclass(to)))
}

wk_geoarrow_schema <- function(x, type_constructor, ...) {
  if (inherits(x, c("nanoarrow_array", "nanoarrow_array_stream"))) {
    schema <- nanoarrow::infer_nanoarrow_schema(x)
    parsed <- geoarrow_schema_parse(schema)
    crs <- if (parsed$crs_type != enum$CrsType$NONE) parsed$crs
    edges <- parsed$edge_type
  } else {
    crs <- wk::wk_crs(x)
    edges <- if (wk::wk_is_geodesic(x)) "SPHERICAL" else "PLANAR"
  }

  type_constructor(..., crs = crs, edges = edges)
}
