# Convert an object to a GeoArrow array

Convert an object to a GeoArrow array

## Usage

``` r
as_geoarrow_array(x, ..., schema = NULL)

as_geoarrow_array_stream(x, ..., schema = NULL)
```

## Arguments

- x:

  An object

- ...:

  Passed to S3 methods

- schema:

  A geoarrow extension schema to use as the target type

## Value

A
[nanoarrow_array](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array.html).

## Examples

``` r
as_geoarrow_array(wk::wkt("POINT (0 1)"))
#> <nanoarrow_array geoarrow.wkt{string}[1]>
#>  $ length    : int 1
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 3
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   ..$ :<nanoarrow_buffer data_offset<int32>[2][8 b]> `0 11`
#>   ..$ :<nanoarrow_buffer data<string>[11 b]> `POINT (0 1)`
#>  $ dictionary: NULL
#>  $ children  : list()
```
