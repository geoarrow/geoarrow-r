# Handler/writer interface for GeoArrow arrays

Handler/writer interface for GeoArrow arrays

## Usage

``` r
geoarrow_handle(x, handler, size = NA_integer_)

geoarrow_writer(schema)
```

## Arguments

- x:

  An object implementing
  [`as_geoarrow_array_stream()`](https://geoarrow.org/geoarrow-r/reference/as_geoarrow_array.md)

- handler:

  A [wk
  handler](https://paleolimbot.github.io/wk/reference/wk_handle.html)

- size:

  The number of elements in the stream or NA if unknown

- schema:

  A
  [nanoarrow_schema](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_schema.html)

## Value

- `geoarrow_handle()`: Returns the result of `handler`

- `geoarrow_writer()`: Returns a [nanoarrow
  array](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array.html)

## Examples

``` r
geoarrow_handle(wk::xy(1:3, 2:4), wk::wk_debug_filter())
#> initialize (dirty = 0  -> 1)
#> vector_start: POINT[UNKNOWN] <0x55584c38f1d0> => WK_CONTINUE
#>   feature_start (1): <0x55584c38f1d0>  => WK_CONTINUE
#>     geometry_start (<none>): POINT[UNKNOWN] <0x55584d0b6b70> => WK_CONTINUE
#>       coord (1): <0x55584d0b6b70> (1.000000 2.000000)  => WK_CONTINUE
#>     geometry_end (<none>)  => WK_CONTINUE
#>   feature_end (1): <0x55584c38f1d0>  => WK_CONTINUE
#>   feature_start (2): <0x55584c38f1d0>  => WK_CONTINUE
#>     geometry_start (<none>): POINT[UNKNOWN] <0x55584d0b6b70> => WK_CONTINUE
#>       coord (1): <0x55584d0b6b70> (2.000000 3.000000)  => WK_CONTINUE
#>     geometry_end (<none>)  => WK_CONTINUE
#>   feature_end (2): <0x55584c38f1d0>  => WK_CONTINUE
#>   feature_start (3): <0x55584c38f1d0>  => WK_CONTINUE
#>     geometry_start (<none>): POINT[UNKNOWN] <0x55584d0b6b70> => WK_CONTINUE
#>       coord (1): <0x55584d0b6b70> (3.000000 4.000000)  => WK_CONTINUE
#>     geometry_end (<none>)  => WK_CONTINUE
#>   feature_end (3): <0x55584c38f1d0>  => WK_CONTINUE
#> vector_end: <0x55584c38f1d0>
#> deinitialize
#> NULL
wk::wk_handle(wk::xy(1:3, 2:4), geoarrow_writer(na_extension_wkt()))
#> <nanoarrow_array geoarrow.wkt{string}[3]>
#>  $ length    : int 3
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 3
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   ..$ :<nanoarrow_buffer data_offset<int32>[4][16 b]> `0 11 22 33`
#>   ..$ :<nanoarrow_buffer data<string>[33 b]> `POINT (1 2)POINT (2 3)POINT (3 4)`
#>  $ dictionary: NULL
#>  $ children  : list()
```
