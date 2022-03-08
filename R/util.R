
`%||%` <- function(lhs, rhs) {
  if (is.null(lhs)) rhs else lhs
}

new_data_frame <- function(x, nrow = df_col_size(x[[1]])) {
  structure(x, row.names = c(NA, nrow), class = "data.frame")
}

df_col_size <- function(x) {
  if (is.data.frame(x)) nrow(x) else length(x)
}

scalar_chr <- function(x) {
  stopifnot(is.character(x), length(x) == 1, !is.na(x))
  x
}

scalar_lgl <- function(x) {
  stopifnot(is.logical(x), length(x) == 1, !is.na(x))
  x
}
