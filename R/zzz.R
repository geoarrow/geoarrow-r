
# nocov start
.onLoad <- function(...) {
  vctr_classes <- c(
    "wkb", "wkt",
    "point", "linestring", "polygon",
    "multipoint", "multilinestring", "multipolygon"
  )

  for (ext in c("wkb", "wkt", "point", "linestring", "polygon", "multi")) {
    cls <- paste0("narrow_vctr_", ext)
    s3_register("vctrs::vec_proxy", cls, vctr_proxy)
    s3_register("vctrs::vec_restore", cls, vctr_restore)
  }

  if (has_arrow_with_extension_type()) {
    try(register_arrow_extension_type(), silent = TRUE)

    s3_register("arrow::as_arrow_table", "sf")

    s3_register("arrow::as_arrow_array", "narrow_vctr_geoarrow")
    s3_register("arrow::infer_type", "narrow_vctr_geoarrow")

    for (cls in supported_handleable_classes) {
      s3_register("arrow::as_arrow_array", cls, as_arrow_array_handleable)
      s3_register("arrow::infer_type", cls, infer_type_handleable)
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
