
GeoArrowType <- list()
GeoArrowType$create <- function(...) {
  stop("Package 'arrow' must be installed to use GeoArrowType")
}


has_arrow_extension_type <- function() {
  inherits(GeoArrowType, "R6ClassGenerator")
}

register_arrow_extension_type <- function() {
  GeoArrowType <<- R6::R6Class(
    "GeoArrowType", inherit = arrow::ExtensionType,
    public = list(
      Deserialize = function() {
        private$schema <- narrow::as_narrow_schema(self)
      },
      as_vector = function(array) {
        wk_handle_wrapper(narrow::as_narrow_array_stream(array), NULL)
      },
      ToString = function() {
        label <- gsub("^geoarrow\\.", "", self$extension_name())

        crs <- self$crs
        if (is.null(crs) || identical(crs, "")) {
          crs <- "<unspecified>"
        } else if (nchar(crs) > 30) {
          crs <- paste0(substr(crs, 1, 27), "...")
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
