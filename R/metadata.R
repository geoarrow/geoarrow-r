
#' Extract Arrow extension type  metadata
#'
#' @param schema A [narrow_schema()]
#'
#' @return A `list()`
#' @export
#'
#' @examples
#' geoarrow_metadata(geoarrow_schema_point(crs = "OGC:CRS84"))
#'
geoarrow_metadata <- function(schema) {
  serialized <- schema$metadata[["ARROW:extension:metadata"]]
  if (is.null(serialized)) list() else geoarrow_metadata_deserialize(serialized)
}

geoarrow_set_metadata <- function(schema, crs, geodesic, edges) {
  meta <- geoarrow_metadata(schema)
  if (!missing(crs)) {
    meta$crs <- crs
  }

  if (!missing(geodesic)) {
    meta$geodesic <- geodesic
  }

  if (!missing(edges)) {
    meta$edges <- edges
  }

  serialized <- do.call(geoarrow_metadata_serialize, meta)
  schema$metadata[["ARROW:extension:metadata"]] <- serialized
  schema
}

geoarrow_copy_metadata <- function(schema_to, schema_from) {
  schema_to$metadata <- schema_from$metadata
  for (i in seq_along(schema_from$children)) {
    schema_to$children[[i]] <- geoarrow_copy_metadata(
      schema_to$children[[i]],
      schema_from$children[[i]]
    )
  }

  schema_to
}

geoarrow_metadata_serialize <- function(crs = NULL, geodesic = NULL, edges = NULL) {
  if (!is.null(geodesic)) {
    geodesic <- scalar_lgl(geodesic)
    geodesic <- if (geodesic) "true" else NULL
  }

  if (length(crs) == 1 && is.na(crs)) {
    crs <- NULL
  }

  vals <- list(crs = crs, geodesic = geodesic, edges = edges)
  vals <- vals[!vapply(vals, is.null, logical(1))]
  vals <- lapply(vals, scalar_chr)

  # will be a better way!
  tmp <- tempfile()
  con <- file(tmp, open = "w+b")
  on.exit({close(con); unlink(tmp)})

  writeBin(length(vals), con, size = 4L)
  for (i in seq_along(vals)) {
    key <- charToRaw(enc2utf8(names(vals)[i]))
    value <- charToRaw(enc2utf8(vals[[i]]))

    writeBin(length(key), con, size = 4L)
    writeBin(key, con)
    writeBin(length(value), con, size = 4L)
    writeBin(value, con)
  }

  n_bytes <- seek(con)
  seek(con, 0L)
  readBin(con, what = raw(), n = n_bytes)
}

geoarrow_metadata_deserialize <- function(metadata) {
  con <- rawConnection(metadata)
  on.exit(close(con))

  out_length <- readBin(con, integer(), size = 4L)
  out <- vector(mode = "list", length = out_length)
  out_names <- character(out_length)

  for (i in seq_along(out)) {
    key_nchar <- readBin(con, integer(), size = 4L)
    out_names[i] <- iconv(
      list(readBin(con, raw(), key_nchar)),
      from = "UTF-8", to = "UTF-8", mark = TRUE
    )[[1]]

    value_nchar <- readBin(con, integer(), size = 4L)
    out[[i]] <- iconv(
      list(readBin(con, raw(), value_nchar)),
      from = "UTF-8", to = "UTF-8", mark = TRUE
    )[[1]]
  }

  names(out) <- out_names
  out
}
