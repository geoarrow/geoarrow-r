
as_arrow_array.geoarrow_vctr <- function(x, ..., type = NULL) {
  if (is.null(type)) {
    array <- narrow::as_narrow_array(x)
    return(narrow::from_narrow_array(array, arrow::Array))
  }

  schema <- attr(x, "schema", exact = TRUE)
  schema_to <- narrow::as_narrow_schema(type)

  schemas_identical <- identical(
    narrow::narrow_schema_info(schema, recursive = TRUE),
    narrow::narrow_schema_info(schema_to, recursive = TRUE)
  )

  if (schemas_identical) {
    array <- narrow::as_narrow_array(x)
    return(narrow::from_narrow_array(array, arrow::Array))
  }

  array <- geoarrow_create_narrow(x, schema = schema_to, strict = TRUE)
  narrow::from_narrow_array(array, arrow::Array)
}

as_chunked_array.geoarrow_vctr <- function(x, ..., type = NULL) {
  schema <- attr(x, "schema", exact = TRUE)
  schema_to <- if(!is.null(type)) narrow::as_narrow_schema(type)

  schemas_identical <- !is.null(type) && identical(
    narrow::narrow_schema_info(schema, recursive = TRUE),
    narrow::narrow_schema_info(schema_to, recursive = TRUE)
  )

  if (is.null(type) || schemas_identical) {
    arrays <- lapply(attr(x, "array_data", exact = TRUE), function(datum) {
      narrow::from_narrow_array(
        narrow::narrow_array(schema, datum),
        arrow::Array
      )
    })

    return(arrow::chunked_array(!!! arrays, type = arrow::infer_type(x)))
  }

  array <- geoarrow_create_narrow(x, schema = schema_to, strict = TRUE)
  arrow::as_chunked_array(narrow::from_narrow_array(array, arrow::Array))
}

infer_type.geoarrow_vctr <- function(x, ...) {
  ptr <- narrow::narrow_allocate_schema()

  narrow::narrow_pointer_export(
    attr(x, "schema", exact = TRUE),
    ptr
  )

  asNamespace("arrow")$DataType$import_from_c(ptr)
}

#' @export
as_geoarrow.Array <- function(x, ..., ptype = NULL, schema_override = NULL) {
  array <- narrow::as_narrow_array(x)
  if (!is.null(schema_override)) {
    array$schema <- schema_override
  }

  as_geoarrow(array, ptype = ptype)
}

#' @export
as_geoarrow.ChunkedArray <- function(x, ..., ptype = NULL, schema_override = NULL) {
  if (is.null(ptype)) {
    schema <- schema_override %||% narrow::as_narrow_schema(x$type)
    cls <- gsub("\\.", "_", schema$metadata[["ARROW:extension:name"]])
    stopifnot(grepl("^geoarrow_", cls))

    arrays <- lapply(x$chunks, narrow::as_narrow_array)

    new_geoarrow_vctr(
      schema,
      lapply(arrays, "[[", "array_data"),
      cls
    )
  } else {
    NextMethod()
  }
}

# deal with classes from dependencies to eliminate the need for a
# circular dependency (actually register in zzz.R)
supported_handleable_classes <- c(
  "sfc", "sfg",
  "wk_wkb", "wk_wkt", "wk_xy", "wk_crc", "wk_rct"
)

as_arrow_array_handleable <- function(x, ..., type = NULL) {
  if (is.null(type)) {
    array <- geoarrow_create_narrow(x)
  } else {
    array <- geoarrow_create_narrow(x, schema = type, strict = TRUE)
  }

  narrow::from_narrow_array(array, arrow::Array)
}

infer_type_handleable <- function(x, ...) {
  ptr <- narrow::narrow_allocate_schema()
  narrow::narrow_pointer_export(
    geoarrow_schema_default(x),
    ptr
  )
  asNamespace("arrow")$DataType$import_from_c(ptr)
}

GeoArrowType <- list()
GeoArrowType$create <- function(...) {
  stop("Package 'arrow' must be installed to use GeoArrowType")
}

has_arrow_extension_type <- function() {
  inherits(GeoArrowType, "R6ClassGenerator")
}

has_arrow_with_extension_type <- function() {
  requireNamespace("arrow", quietly = TRUE) &&
    (utils::packageVersion("arrow") >= "7.0.0.9000")
}

# this runs in .onLoad(), where we can't get coverage
# nocov start
register_arrow_extension_type <- function() {
  # for CMD check
  self <- NULL
  private <- NULL

  GeoArrowType <<- R6::R6Class(
    "GeoArrowType", inherit = arrow::ExtensionType,
    public = list(

      deserialize_instance = function() {
        private$schema <- narrow::as_narrow_schema(self)
      },

      as_vector = function(array) {
        as_geoarrow(array)
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

        edges <- self$edges
        if (is.null(edges)) {
          edges <- ""
        } else {
          edges <- paste0("(", edges, " edges)")
        }

        sprintf("%s %s%s", label, crs, edges)
      }
    ),
    active = list(
      crs = function() {
        recursive_extract_narrow_schema(private$schema, "crs")
      },
      edges = function() {
        recursive_extract_narrow_schema(private$schema, "edges")
      }
    ),
    private = list(
      schema = NULL
    )
  )

  # This shouldn't be needed directly...these objects will get instantiated
  # when the Type object gets surfaced to R provided that the extension types
  # have been registered.
  GeoArrowType$create <- function(schema) {
    schema <- narrow::as_narrow_schema(schema)

    # this is a bit of a hack and could probably be done better in narrow
    ext <- scalar_chr(schema$metadata[["ARROW:extension:name"]])
    metadata <- schema$metadata[["ARROW:extension:metadata"]]
    schema$metadata[c("ARROW:extension:name", "ARROW:extension:metadata")] <- NULL
    dummy_array <- narrow::narrow_array(schema, validate = FALSE)
    storage_type <- narrow::from_narrow_array(
      dummy_array,
      # ...because arrow::DataType is not exported
      asNamespace("arrow")$DataType
    )

    arrow::new_extension_type(
      storage_type = storage_type,
      extension_name = ext,
      extension_metadata = metadata,
      type_class = GeoArrowType
    )
  }

  representative_schemas <- list(
    geoarrow_schema_wkb(),
    geoarrow_schema_wkt(),
    geoarrow_schema_point(),
    geoarrow_schema_linestring(),
    geoarrow_schema_polygon(),
    geoarrow_schema_multipoint(),
    geoarrow_schema_multilinestring(),
    geoarrow_schema_multipolygon()
  )

  for (schema in representative_schemas) {
    arrow::reregister_extension_type(
      GeoArrowType$create(schema)
    )
  }
}
# nocov end
