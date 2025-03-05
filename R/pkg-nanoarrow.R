
# Runs before coverage starts on load
# nocov start
register_geoarrow_extension <- function() {
  for (ext_name in all_extension_names()) {
    nanoarrow::register_nanoarrow_extension(
      ext_name,
      nanoarrow::nanoarrow_extension_spec(subclass = "geoarrow_extension_spec")
    )
  }
}
# nocov end

#' @importFrom nanoarrow infer_nanoarrow_ptype_extension
#' @export
infer_nanoarrow_ptype_extension.geoarrow_extension_spec <- function(extension_spec,
                                                                    x, ...) {
  nanoarrow::nanoarrow_vctr(schema = x, subclass = "geoarrow_vctr")
}

#' @importFrom nanoarrow convert_array_extension
#' @export
convert_array_extension.geoarrow_extension_spec <- function(extension_spec,
                                                            array, to, ...) {
  # For the default, this will dispatch to convert_array.geoarrow_vctr().
  # This gets called if to is a base R type (e.g., integer())
  stop(
    sprintf(
      "Can't convert geoarrow extension array to object of class '%s'",
      class(to)[1]
    )
  )
}

#' @importFrom nanoarrow as_nanoarrow_array_extension
#' @export
as_nanoarrow_array_extension.geoarrow_extension_spec <- function(
    extension_spec, x, ..., schema = NULL) {
  as_geoarrow_array(x, schema = schema)
}

#' @importFrom nanoarrow convert_array
#' @export
convert_array.geoarrow_vctr <- function(array, to, ...) {
  as_geoarrow_vctr(array, schema = nanoarrow::as_nanoarrow_schema(to))
}

all_extension_names <- function() {
  c(
    "geoarrow.wkt", "geoarrow.wkb", "geoarrow.point", "geoarrow.linestring",
    "geoarrow.polygon", "geoarrow.multipoint", "geoarrow.mutlilinestring",
    "geoarrow.multipolygon", "geoarrow.box"
  )
}
