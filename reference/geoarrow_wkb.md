# GeoArrow Types

These functions provide GeoArrow type definitions as zero-length
vectors.

## Usage

``` r
geoarrow_wkb(crs = NULL, edges = "PLANAR")

geoarrow_wkt(crs = NULL, edges = "PLANAR")

geoarrow_large_wkb(crs = NULL, edges = "PLANAR")

geoarrow_large_wkt(crs = NULL, edges = "PLANAR")

geoarrow_wkb_view(crs = NULL, edges = "PLANAR")

geoarrow_wkt_view(crs = NULL, edges = "PLANAR")

geoarrow_native(
  geometry_type,
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)

geoarrow_point(
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)

geoarrow_linestring(
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)

geoarrow_polygon(
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)

geoarrow_multipoint(
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)

geoarrow_multilinestring(
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)

geoarrow_multipolygon(
  dimensions = "XY",
  coord_type = "SEPARATE",
  crs = NULL,
  edges = "PLANAR"
)

geoarrow_box(
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
[geoarrow_vctr](https://geoarrow.org/geoarrow-r/reference/as_geoarrow_vctr.md)

## Examples

``` r
geoarrow_wkb()
#> <geoarrow_vctr geoarrow.wkb{binary}[0]>
#> character(0)
geoarrow_wkt()
#> <geoarrow_vctr geoarrow.wkt{string}[0]>
#> character(0)
geoarrow_point()
#> <geoarrow_vctr geoarrow.point{struct}[0]>
#> character(0)
```
