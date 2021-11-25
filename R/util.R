
scalar_chr <- function(x) {
  stopifnot(is.character(x), length(x) == 1, !is.na(x))
  x
}

scalar_lgl <- function(x) {
  stopifnot(is.logical(x), length(x) == 1, !is.na(x))
  x
}
