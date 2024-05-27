
# nocov start
.onLoad <- function(...) {
  register_geoarrow_extension()
  register_arrow_ext_or_set_hook()

  s3_register("sf::st_as_sfc", "geoarrow_vctr")
  s3_register("sf::st_as_sfc", "ChunkedArray")
  s3_register("sf::st_as_sfc", "Array")
  s3_register("sf::st_as_sf", "ArrowTabular")
  s3_register("sf::st_as_sf", "Dataset")
  s3_register("sf::st_as_sf", "Scanner")
  s3_register("sf::st_as_sf", "RecordBatchReader")
  s3_register("sf::st_as_sf", "arrow_dplyr_query")
  s3_register("arrow::as_arrow_array", "geoarrow_vctr")
  s3_register("arrow::as_arrow_array", "sfc")
  s3_register("arrow::as_chunked_array", "geoarrow_vctr")
  s3_register("arrow::as_chunked_array", "sfc")
  s3_register("arrow::infer_type", "geoarrow_vctr")
  s3_register("arrow::infer_type", "sfc")
}

# From the `vctrs` package (this function is intended to be copied
# without attribution or license requirements to avoid a hard dependency on
# vctrs:
# nolint https://github.com/r-lib/vctrs/blob/c2a7710fe55e3a2249c4fdfe75bbccbafcf38804/R/register-s3.R#L25-L31
s3_register <- function(generic, class, method = NULL) {
  stopifnot(is.character(generic), length(generic) == 1)
  stopifnot(is.character(class), length(class) == 1)

  pieces <- strsplit(generic, "::")[[1]]
  stopifnot(length(pieces) == 2)
  package <- pieces[[1]]
  generic <- pieces[[2]]

  caller <- parent.frame()

  get_method_env <- function() {
    top <- topenv(caller)
    if (isNamespace(top)) {
      asNamespace(environmentName(top))
    } else {
      caller
    }
  }
  get_method <- function(method, env) {
    if (is.null(method)) {
      get(paste0(generic, ".", class), envir = get_method_env())
    } else {
      method
    }
  }

  register <- function(...) {
    envir <- asNamespace(package)

    # Refresh the method each time, it might have been updated by
    # `devtools::load_all()`
    method_fn <- get_method(method)
    stopifnot(is.function(method_fn))


    # Only register if generic can be accessed
    if (exists(generic, envir)) {
      registerS3method(generic, class, method_fn, envir = envir)
    } else if (identical(Sys.getenv("NOT_CRAN"), "true")) {
      warning(sprintf(
        "Can't find generic `%s` in package %s to register S3 method.",
        generic,
        package
      ))
    }
  }

  # Always register hook in case package is later unloaded & reloaded
  setHook(packageEvent(package, "onLoad"), register)

  # Avoid registration failures during loading (pkgload or regular)
  if (isNamespaceLoaded(package)) {
    register()
  }

  invisible()
}
# nocov end
