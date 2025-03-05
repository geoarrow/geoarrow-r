
as_arrow_array.geoarrow_vctr <- function(x, ..., type = NULL) {
  chunked <- as_chunked_array.geoarrow_vctr(x, ..., type = type)
  if (chunked$num_chunks == 1) {
    chunked$chunk(0)
  } else {
    arrow::as_arrow_array(chunked)
  }
}

#' @importFrom nanoarrow as_nanoarrow_schema
as_chunked_array.geoarrow_vctr <- function(x, ..., type = NULL) {
  if (is.null(type)) {
    schema <- NULL
    type <- arrow::as_data_type(attr(x, "schema", exact = TRUE))
  } else {
    schema <- nanoarrow::as_nanoarrow_schema(type)
    type <- arrow::as_data_type(type)
  }

  # as_nanoarrow_array_stream() applies the indices if vctr is sliced
  stream <- as_geoarrow_array_stream(x, schema = schema)
  chunks <- nanoarrow::collect_array_stream(stream, validate = FALSE)
  type <- arrow::as_data_type(type)

  schema <- nanoarrow::as_nanoarrow_schema(type)
  arrays <- vector("list", length(chunks))
  for (i in seq_along(arrays)) {
    tmp_schema <- nanoarrow::nanoarrow_allocate_schema()
    nanoarrow::nanoarrow_pointer_export(schema, tmp_schema)
    tmp_array <- nanoarrow::nanoarrow_allocate_array()
    nanoarrow::nanoarrow_pointer_export(chunks[[i]], tmp_array)
    arrays[[i]] <- arrow::Array$import_from_c(tmp_array, tmp_schema)
  }

  arrow::chunked_array(!!!arrays, type = type)
}

infer_type.geoarrow_vctr <- function(x, ...) {
  arrow::as_data_type(nanoarrow::as_nanoarrow_schema(x))
}

#' @export
as_geoarrow_array_stream.ChunkedArray <- function(x, ..., schema = NULL) {
  stream <- nanoarrow::basic_array_stream(
    lapply(x$chunks, nanoarrow::as_nanoarrow_array),
    schema = nanoarrow::as_nanoarrow_schema(x$type),
    validate = FALSE
  )

  as_geoarrow_array_stream(stream, schema = schema)
}

#' @export
as_geoarrow_array_stream.Array <- function(x, ..., schema = NULL) {
  stream <- nanoarrow::basic_array_stream(
    list(nanoarrow::as_nanoarrow_array(x)),
    schema = nanoarrow::as_nanoarrow_schema(x$type),
    validate = FALSE
  )

  as_geoarrow_array_stream(stream, schema = schema)
}

GeometryExtensionType <- new.env(parent = emptyenv())
GeometryExtensionType$create <- function(...) {
  stop("Package 'arrow' must be loaded to use GeometryExtensionType")
}

# this runs in .onLoad(), where we can't get coverage
# nocov start
register_arrow_ext_or_set_hook <- function(...) {
  # Register a hook for the arrow package being loaded to run the extension type
  # registration
  setHook(packageEvent("arrow", "onLoad"), register_arrow_extension_type)

  # If arrow is already loaded, run the registration now
  if (isNamespaceLoaded("arrow")) {
    register_arrow_extension_type()
  }
}

register_arrow_extension_type <- function(...) {
  # for CMD check
  self <- NULL
  private <- NULL

  GeometryExtensionType$cls <- R6::R6Class(
    "GeometryExtensionType", inherit = arrow::ExtensionType,
    public = list(

      deserialize_instance = function() {
        private$schema <- nanoarrow::as_nanoarrow_schema(self)
        private$parsed <- geoarrow_schema_parse(private$schema)
      },

      as_vector = function(array) {
        as_geoarrow_vctr(array)
      },

      ToString = function() {
        label <- self$extension_name()

        crs <- self$crs
        if (is.null(crs) || identical(crs, "")) {
          crs <- "<crs: unspecified>"
        } else if (nchar(crs) > 30) {
          crs <- paste0("<CRS: ", substr(crs, 1, 27), "...")
        } else {
          crs <- paste0("<CRS: ", crs, ">")
        }

        if (self$edge_type != "PLANAR") {
          label <- paste(tolower(self$edge_type), label)
        }

        sprintf("%s %s", label, crs)
      }
    ),
    active = list(
      geoarrow_id = function() {
        private$parsed$id
      },
      geometry_type = function() {
        private$parsed$geometry_type
      },
      dimensions = function() {
        enum_label(private$parsed$dimensions, "Dimensions")
      },
      coord_type = function() {
        enum_label(private$parsed$coord_type, "CoordType")
      },
      crs = function() {
        if (private$parsed$crs_type == enum$CrsType$NONE) {
          NULL
        } else {
          private$parsed$crs
        }
      },
      edge_type = function() {
        enum_label(private$parsed$edge_type, "EdgeType")
      }
    ),
    private = list(
      schema = NULL,
      parsed = NULL
    )
  )

  # This shouldn't be needed directly...these objects will get instantiated
  # when the Type object gets surfaced to R provided that the extension types
  # have been registered.
  GeometryExtensionType$create <- function(schema) {
    schema <- nanoarrow::as_nanoarrow_schema(schema)
    parsed <- geoarrow_schema_parse(schema)

    arrow::new_extension_type(
      storage_type = arrow::as_data_type(force_schema_storage(schema)),
      extension_name = parsed$extension_name,
      extension_metadata = "",
      type_class = GeometryExtensionType$cls
    )
  }

  representative_schemas <- list(
    na_extension_wkt(),
    na_extension_large_wkt(),
    na_extension_wkb(),
    na_extension_large_wkb(),
    na_extension_geoarrow(enum$GeometryType$POINT),
    na_extension_geoarrow(enum$GeometryType$LINESTRING),
    na_extension_geoarrow(enum$GeometryType$POLYGON),
    na_extension_geoarrow(enum$GeometryType$MULTIPOINT),
    na_extension_geoarrow(enum$GeometryType$MULTILINESTRING),
    na_extension_geoarrow(enum$GeometryType$MULTIPOLYGON),
    na_extension_geoarrow(enum$GeometryType$BOX)
  )

  for (schema in representative_schemas) {
    arrow::reregister_extension_type(GeometryExtensionType$create(schema))
  }
}
# nocov end
