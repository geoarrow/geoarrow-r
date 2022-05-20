
# nocov start
.onLoad <- function(...) {
  s3_register("sf::st_as_sfc", "geoarrow_vctr")
  s3_register("sf::st_geometry", "geoarrow_vctr")
  s3_register("sf::st_crs", "geoarrow_vctr")
  s3_register("sf::st_bbox", "geoarrow_vctr")

  if (has_arrow_with_extension_type()) {
    try(register_arrow_extension_type(), silent = TRUE)

    s3_register("arrow::as_arrow_table", "sf")

    s3_register("arrow::as_arrow_array", "geoarrow_vctr")
    s3_register("arrow::as_chunked_array", "geoarrow_vctr")
    s3_register("arrow::infer_type", "geoarrow_vctr")

    for (cls in supported_handleable_classes) {
      s3_register("arrow::as_arrow_array", cls, as_arrow_array_handleable)
      s3_register("arrow::infer_type", cls, infer_type_handleable)
    }

    table_like_things <- c("arrow_dplyr_query", "RecordBatch", "Table", "Dataset")
    for (cls in table_like_things) {
      s3_register("sf::st_as_sf", cls, st_as_sf.ArrowTabular)
      s3_register("sf::st_geometry", cls, st_geometry.ArrowTabular)
      s3_register("sf::st_bbox", cls, st_bbox.ArrowTabular)
      s3_register("sf::st_crs", cls, st_crs.ArrowTabular)
    }
  }
}

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
