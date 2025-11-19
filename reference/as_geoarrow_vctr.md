# GeoArrow encoded arrays as R vectors

GeoArrow encoded arrays as R vectors

## Usage

``` r
as_geoarrow_vctr(x, ..., schema = NULL)
```

## Arguments

- x:

  An object that works with
  [`as_geoarrow_array_stream()`](https://geoarrow.org/geoarrow-r/reference/as_geoarrow_array.md).
  Most spatial objects in R already work with this method.

- ...:

  Passed to
  [`as_geoarrow_array_stream()`](https://geoarrow.org/geoarrow-r/reference/as_geoarrow_array.md)

- schema:

  An optional `schema` (e.g.,
  [`na_extension_geoarrow()`](https://geoarrow.org/geoarrow-r/reference/na_extension_wkb.md)).

## Value

A vctr of class 'geoarrow_vctr'

## Examples

``` r
as_geoarrow_vctr("POINT (0 1)")
#> <geoarrow_vctr geoarrow.wkt{string}[1]>
#> [1] <POINT (0 1)>
```
