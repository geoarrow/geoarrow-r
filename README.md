
<!-- README.md is generated from README.Rmd. Please edit that file -->

# geoarrow

<!-- badges: start -->

[![R-CMD-check](https://github.com/paleolimbot/geoarrow/workflows/R-CMD-check/badge.svg)](https://github.com/paleolimbot/geoarrow/actions)
[![Codecov test
coverage](https://codecov.io/gh/paleolimbot/geoarrow/branch/master/graph/badge.svg)](https://codecov.io/gh/paleolimbot/geoarrow?branch=master)
<!-- badges: end -->

The goal of geoarrow is to prototype Arrow representations of geometry.
This is currently a [first-draft
specification](https://github.com/jorisvandenbossche/geo-arrow-spec/blob/geo-arrow-format-initial/format.md)
and nothing here should be used for anything except entertainment value.

## Installation

You can install the development version from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("paleolimbot/geoarrow")
```

## Type examples

``` r
library(geoarrow)
```

First, specs for the kind of data people are already using:

``` r
pretty_type(geo_arrow_schema_wkb())
#> Binary
#> binary
pretty_type(geo_arrow_schema_wkt())
#> Utf8
#> string
pretty_type(geo_arrow_schema_geojson())
#> Utf8
#> string
```

…but also Arrow-native forms that don’t need to be parsed:

``` r
pretty_type(geo_arrow_schema_point())
#> FixedSizeListType
#> fixed_size_list<: double not null>[2]
pretty_type(geo_arrow_schema_point_float32())
#> FixedSizeListType
#> fixed_size_list<: float not null>[2]
pretty_type(geo_arrow_schema_linestring())
#> ListType
#> list<: fixed_size_list<: double not null>[2] not null>
pretty_type(geo_arrow_schema_polygon())
#> ListType
#> list<: list<: fixed_size_list<: double not null>[2] not null> not null>
pretty_type(geo_arrow_schema_multi(geo_arrow_schema_point()))
#> ListType
#> list<: fixed_size_list<: double not null>[2]>
pretty_type(geo_arrow_schema_multi(geo_arrow_schema_linestring()))
#> ListType
#> list<: list<: fixed_size_list<: double not null>[2] not null>>
pretty_type(geo_arrow_schema_multi(geo_arrow_schema_polygon()))
#> ListType
#> list<: list<: list<: fixed_size_list<: double not null>[2] not null> not null>>
# (I can't get dense or sparse unions to import to the Arrow R package at the moment
# but that's how collections could be stored)
```

This setup gives considerable flexibility as to the coordinate storage
format (with a reasonable default whose usage could be mandated for a
particular library). For example, s2 cell IDs could be used for points
using the same linestring type:

``` r
pretty_type(
  geo_arrow_schema_linestring(
    point = geo_arrow_schema_point_s2()
  )
)
#> ListType
#> list<: uint64>
```

In addition to nested list types, also define “flat” types that can be
streamed without waiting for a feature boundary. This is helpful for
algorithms that don’t need all the coordinates in memory at once or for
streaming very large geometries over a network connection.

``` r
pretty_type(geo_arrow_schema_flat_linestring())
#> StructType
#> struct<linestring_id: int32, : fixed_size_list<: double not null>[2] not null>
pretty_type(geo_arrow_schema_flat_polygon())
#> StructType
#> struct<polygon_id: int32, ring_id: int32, : fixed_size_list<: double not null>[2] not null>
pretty_type(geo_arrow_schema_flat_multi(geo_arrow_schema_point()))
#> StructType
#> struct<multi_id: int32, : fixed_size_list<: double not null>[2]>
```

The schemas here use column-level extension types and extension metadata
to encode dimension names, CRS information, and a flag to specify that
edges should be considered ellipsoidal rather than Cartesian.

``` r
schema <- geo_arrow_schema_linestring(ellipsoidal = TRUE)
schema$metadata[["ARROW:extension:name"]]
#> [1] "geo_arrow_linestring"
geo_arrow_metadata(schema)
#> $ellipsoidal
#> [1] "true"
geo_arrow_metadata(schema$children[[1]])
#> $dim
#> [1] "xy"
```
