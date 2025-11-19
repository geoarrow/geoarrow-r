# Extension type definitions for GeoArrow extension types

Extension type definitions for GeoArrow extension types

## Usage

``` r
na_extension_wkb(crs = NULL, edges = "PLANAR")

na_extension_wkt(crs = NULL, edges = "PLANAR")

na_extension_large_wkb(crs = NULL, edges = "PLANAR")

na_extension_large_wkt(crs = NULL, edges = "PLANAR")

na_extension_wkb_view(crs = NULL, edges = "PLANAR")

na_extension_wkt_view(crs = NULL, edges = "PLANAR")

na_extension_geoarrow(
  geometry_type,
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)
```

## Arguments

- crs:

  An object representing a CRS. For maximum portability, it should
  implement
  [`wk::wk_crs_projjson()`](https://paleolimbot.github.io/wk/reference/wk_crs_proj_definition.html).

- edges:

  One of "PLANAR" or "SPHERICAL".

- geometry_type:

  One of "POINT", "LINESTRING", "POLYGON", "MULTIPOINT",
  "MULTILINESTRING", "MULTIPOLYGON".

- dimensions:

  One of "XY", "XYZ", "XYM", or "XYZM"

- coord_type:

  One of "SEPARATE" or "INTERLEAVED"

## Value

A
[nanoarrow_schema](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_schema.html).

## Examples

``` r
na_extension_wkb(crs = "OGC:CRS84")
#> <nanoarrow_schema geoarrow.wkb{binary}>
#>  $ format    : chr "z"
#>  $ name      : NULL
#>  $ metadata  :List of 2
#>   ..$ ARROW:extension:name    : chr "geoarrow.wkb"
#>   ..$ ARROW:extension:metadata: chr "{\"crs\":{\"$schema\":\"https://proj.org/schemas/v0.7/projjson.schema.json\",\"type\":\"GeographicCRS\",\"name\"| __truncated__
#>  $ flags     : int 2
#>  $ children  : list()
#>  $ dictionary: NULL
na_extension_geoarrow("POINT")
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
