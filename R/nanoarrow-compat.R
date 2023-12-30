
register_geoarrow_extension <- function() {
  for (ext_name in geoarrow_extension_name_all()) {
    nanoarrow::register_nanoarrow_extension(
      ext_name,
      nanoarrow::nanoarrow_extension_spec(subclass = "geoarrow_extension_spec")
    )
  }
}

#' @importFrom nanoarrow infer_nanoarrow_ptype_extension
#' @export
infer_nanoarrow_ptype_extension.geoarrow_extension_spec <- function(extension_spec, x, ...) {
  new_geoarrow_vctr(list(), x, integer())
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
  as_geoarrow_vctr(array, schema = as_nanoarrow_schema(to))
}
