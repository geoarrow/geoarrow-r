# Infer a GeoArrow-native type from a vector

Infer a GeoArrow-native type from a vector

## Usage

``` r
infer_geoarrow_schema(x, ..., promote_multi = TRUE, coord_type = NULL)
```

## Arguments

- x:

  An object from which to infer a schema.

- ...:

  Passed to S3 methods.

- promote_multi:

  Use `TRUE` to return a MULTI type when both normal and MULTI elements
  are in the same array.

- coord_type:

  Specify the coordinate type to use if returning

## Value

A
[nanoarrow_schema](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_schema.html)

## Examples

``` r
infer_geoarrow_schema(wk::wkt("POINT (0 1)"))
#> <nanoarrow_schema geoarrow.point{struct}>
#>  $ format    : chr "+s"
#>  $ name      : NULL
#>  $ metadata  :List of 2
#>   ..$ ARROW:extension:name    : chr "geoarrow.point"
#>   ..$ ARROW:extension:metadata: chr "{}"
#>  $ flags     : int 2
#>  $ children  :List of 2
#>   ..$ x:<nanoarrow_schema double>
#>   .. ..$ format    : chr "g"
#>   .. ..$ name      : chr "x"
#>   .. ..$ metadata  : list()
#>   .. ..$ flags     : int 0
#>   .. ..$ children  : list()
#>   .. ..$ dictionary: NULL
#>   ..$ y:<nanoarrow_schema double>
#>   .. ..$ format    : chr "g"
#>   .. ..$ name      : chr "y"
#>   .. ..$ metadata  : list()
#>   .. ..$ flags     : int 0
#>   .. ..$ children  : list()
#>   .. ..$ dictionary: NULL
#>  $ dictionary: NULL
```
