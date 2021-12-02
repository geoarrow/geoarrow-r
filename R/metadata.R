
#' Extract Arrow extension type  metadata
#'
#' @param schema A [carrow_schema()]
#'
#' @return A `list()`
#' @export
#'
#' @examples
#' geoarrow_metadata(geoarrow_schema_point(crs = "OGC:CRS84"))
#'
geoarrow_metadata <- function(schema) {
  geoarrow_metadata_deserialize(schema$metadata[["ARROW:extension:metadata"]])
}

geoarrow_metadata_serialize <- function(crs = NULL, geodesic = NULL, dim = NULL) {
  if (!is.null(geodesic)) {
    geodesic <- scalar_lgl(geodesic)
    geodesic <- if (geodesic) "true" else NULL
  }

  vals <- list(crs = crs, geodesic = geodesic, dim = dim)
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

temporary_to_keep_rmd_check_clean <- function() {
  .Call(geoarrow_c_schema_wkb, 1, 2)
}

