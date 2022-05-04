
#' Write geometry as Apache Parquet files
#'
#' @inheritDotParams arrow::write_parquet
#' @inheritParams geoarrow_create_narrow
#'
#' @return The result of [arrow::write_parquet()], invisibly
#' @export
#'
write_geoarrow_parquet <- function(handleable, ..., schema = NULL, strict = FALSE) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for write_geoarrow_parquet()", call. = FALSE) # nocov
  }

  batch <- geoarrow_make_batch(
    handleable,
    schema,
    strict,
    # workaround because Arrow Parquet can't roundtrip null fixed-width list
    # elements (https://issues.apache.org/jira/browse/ARROW-8228)
    null_point_as_empty = is.null(schema) ||
      startsWith(schema$format, "+w:"),
    geoparquet_metadata = TRUE
  )

  # write!
  arrow::write_parquet(batch, ...)
}

#' @rdname write_geoarrow_parquet
#' @export
write_geoarrow_feather <- function(handleable, ..., schema = NULL, strict = FALSE) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for write_geoarrow_feather()", call. = FALSE) # nocov
  }

  batch <- geoarrow_make_batch(handleable, schema, strict, geoparquet_metadata = TRUE)

  # write!
  arrow::write_feather(batch, ...)
}

#' @rdname write_geoarrow_parquet
#' @export
write_geoarrow_ipc_stream <- function(handleable, ..., schema = NULL, strict = FALSE) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' required for write_geoarrow_ipc_stream()", call. = FALSE) # nocov
  }

  batch <- geoarrow_make_batch(handleable, schema, strict, geoparquet_metadata = TRUE)

  # write!
  arrow::write_ipc_stream(batch, ...)
}

geoarrow_make_batch <- function(handleable, schema = NULL, strict = FALSE,
                                null_point_as_empty = FALSE,
                                geoparquet_metadata = FALSE) {
  if (!is.data.frame(handleable)) {
    handleable <- data.frame(geometry = handleable)
  } else {
    handleable <- as.data.frame(handleable)
  }

  is_handleable <- vapply(handleable, is_handleable_column, logical(1))
  df_attr <- handleable[!is_handleable]
  df_handleable <- handleable[is_handleable]

  if (is.null(schema)) {
    arrays_handleable <- lapply(
      df_handleable,
      geoarrow_create_narrow,
      null_point_as_empty = null_point_as_empty
    )
  } else {
    arrays_handleable <- lapply(
      df_handleable,
      geoarrow_create_narrow,
      schema = schema,
      strict = strict,
      null_point_as_empty = null_point_as_empty
    )
  }

  # create file metadata
  if (geoparquet_metadata) {
    handleable_schema <- narrow::narrow_schema(
      format = "+s",
      children = lapply(arrays_handleable, "[[", "schema")
    )
    for (i in seq_along(handleable_schema$children)) {
      handleable_schema$children[[i]]$name <- names(arrays_handleable)[i]
    }
    file_metadata <- geoparquet_metadata(handleable_schema)

    # add bounding boxes and geometry type lists
    for (col in names(arrays_handleable)) {
      array <- arrays_handleable[[col]]
      if (array$array_data$length == 0) {
        next
      }

      box <- narrow::from_narrow_array(
        geoarrow_compute(array, "global_bounds", list(null_is_empty = TRUE))
      )

      box_has_z <- is.finite(box$zmax - box$zmin)
      box_has_m <- is.finite(box$mmax - box$mmin)
      if (box_has_z && box_has_m) {
        file_metadata$columns[[col]]$bbox <- c(
          box$xmin, box$ymin, box$zmin, box$mmin,
          box$xmax, box$ymax, box$mmax, box$mmax
        )
      } else if (box_has_z) {
        file_metadata$columns[[col]]$bbox <- c(
          box$xmin, box$ymin, box$zmin,
          box$xmax, box$ymax, box$zmax
        )
      } else if (box_has_m) {
        file_metadata$columns[[col]]$bbox <- c(
          box$xmin, box$ymin, box$mmin,
          box$xmax, box$ymax, box$mmax
        )
      } else {
        file_metadata$columns[[col]]$bbox <- c(
          box$xmin, box$ymin,
          box$xmax, box$ymax
        )
      }

      # try to calculate geometry types from wk_vector_meta(),
      # which doesn't iterate along the entire array
      meta <- wk::wk_vector_meta(array)
      geom_type <- wk::wk_geometry_type_label(meta$geometry_type)
      substr(geom_type, 1, 1) <- toupper(substr(geom_type, 1, 1))
      if (is.na(meta$has_z) || is.na(meta$has_m) || is.na(meta$geometry_type)) {
        types_array <- geoarrow_compute(
          geoarrow_create_narrow_from_buffers(
            wk::wkt(c("LINESTRING ZM EMPTY", "LINESTRING (0 0, 1 1)")),
            schema = geoarrow_schema_wkt()
          ),
          "geoparquet_types",
          list(include_empty = FALSE)
        )
        types <- narrow::from_narrow_array(types_array, character())
      } else if (isTRUE(meta$has_z) && isTRUE(meta$has_m)) {
        types <- paste(geom_type, "ZM")
      } else if (isTRUE(meta$has_z)) {
        types <- paste(geom_type, "M")
      } else if (isTRUE(meta$has_m)) {
        types <- paste(geom_type, "M")
      } else {
        types <- geom_type
      }

      file_metadata$columns[[col]]$geometry_type <- types
    }
  } else {
    file_metadata <- NULL
  }

  # create the record batch before shipping to Arrow because this has a better
  # chance of keeping metadata associated with the type
  batch_handleable <- narrow::narrow_array(
    narrow::narrow_schema(
      "+s",
      children = lapply(seq_along(arrays_handleable), function(i) {
        schema <- arrays_handleable[[i]]$schema
        schema$name <- names(arrays_handleable)[i]
        schema
      })),
    narrow::narrow_array_data(
      buffers = list(NULL),
      length = arrays_handleable[[1]]$array_data$length,
      null_count = 0,
      children = lapply(arrays_handleable, "[[", "array_data")
    )
  )

  # create arrow RecordBatches
  batch_attr_arrow <- arrow::record_batch(df_attr)
  batch_handleable_arrow <- narrow::from_narrow_array(batch_handleable, arrow::RecordBatch)

  # combine them making sure the schema contains field-level metadata
  # (with the extension name/metadata)
  arrays <- lapply(colnames(handleable), function(col_name) {
    batch_attr_arrow$GetColumnByName(col_name) %||%
      batch_handleable_arrow$GetColumnByName(col_name)
  })
  names(arrays) <- colnames(handleable)

  fields <- lapply(colnames(handleable), function(col_name) {
    batch_attr_arrow$schema$GetFieldByName(col_name) %||%
      batch_handleable_arrow$schema$GetFieldByName(col_name)
  })
  names(fields) <- colnames(handleable)

  batch <- arrow::record_batch(!!! arrays, schema = arrow::schema(!!! fields))

  # add file metadata
  if (!is.null(file_metadata)) {
    batch$metadata$geo <- jsonlite::toJSON(
      file_metadata,
      null = "null",
      auto_unbox = TRUE
    )
  }

  batch
}

is_handleable_column <- function(x) {
  tryCatch({wk::wk_vector_meta(x); TRUE}, error = function(e) FALSE)
}
