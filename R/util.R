
`%||%` <- function(lhs, rhs) {
  if (is.null(lhs)) rhs else lhs
}

new_data_frame <- function(x, nrow = length(x[[1]])) {
  structure(x, row.names = c(NA, nrow), class = "data.frame")
}

scalar_chr <- function(x) {
  stopifnot(is.character(x), length(x) == 1, !is.na(x))
  x
}

scalar_lgl <- function(x) {
  stopifnot(is.logical(x), length(x) == 1, !is.na(x))
  x
}
