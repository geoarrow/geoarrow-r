
test_that("geoarrow_kernel_call_scalar() works", {
  result <- geoarrow_kernel_call_scalar(
    "void",
    nanoarrow::as_nanoarrow_array(1:5)
  )
  expect_identical(result$get_schema()$format, "n")
  expect_identical(result$get_next()$length, 5L)
})

test_that("geoarrow_kernel_call_agg() works", {
  result <- geoarrow_kernel_call_agg(
    "void_agg",
    nanoarrow::as_nanoarrow_array(1:5)
  )
  expect_identical(result$length, 1L)
})

test_that("void kernel can be created and run", {
  kernel <- geoarrow_kernel("void", list(nanoarrow::na_int32()))
  expect_s3_class(kernel, "geoarrow_kernel_void")
  expect_identical(attr(kernel, "is_agg"), FALSE)
  expect_identical(attr(kernel, "output_type")$format, "n")

  result <- geoarrow_kernel_push(kernel, list(1:5))
  expect_identical(result$length, 5L)
})

test_that("void_agg kernel can be created and run", {
  kernel <- geoarrow_kernel("void_agg", list(nanoarrow::na_int32()))
  expect_identical(attr(kernel, "is_agg"), TRUE)
  expect_identical(attr(kernel, "output_type")$format, "n")

  geoarrow_kernel_push(kernel, list(1:5))
  result <- geoarrow_kernel_finish(kernel)
  expect_identical(result$length, 1L)
})
