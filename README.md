
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

## Write and write to Parquet

This exists for prototyping only, but should work with most things that
you throw at it. Notably, sf objects should work out-of-the-box.

``` r
library(geoarrow)

nc <- sf::read_sf(system.file("shape/nc.shp", package = "sf"))
write_geoarrow_parquet(nc, "nc.parquet")
sf::st_as_sf(read_geoarrow_parquet("nc.parquet"))
#> Simple feature collection with 100 features and 14 fields
#> Geometry type: MULTIPOLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -84.32385 ymin: 33.88199 xmax: -75.45698 ymax: 36.58965
#> Geodetic CRS:  NAD27
#> # A tibble: 100 × 15
#>     AREA PERIMETER CNTY_ CNTY_ID NAME  FIPS  FIPSNO CRESS_ID BIR74 SID74 NWBIR74
#>    <dbl>     <dbl> <dbl>   <dbl> <chr> <chr>  <dbl>    <int> <dbl> <dbl>   <dbl>
#>  1 0.114      1.44  1825    1825 Ashe  37009  37009        5  1091     1      10
#>  2 0.061      1.23  1827    1827 Alle… 37005  37005        3   487     0      10
#>  3 0.143      1.63  1828    1828 Surry 37171  37171       86  3188     5     208
#>  4 0.07       2.97  1831    1831 Curr… 37053  37053       27   508     1     123
#>  5 0.153      2.21  1832    1832 Nort… 37131  37131       66  1421     9    1066
#>  6 0.097      1.67  1833    1833 Hert… 37091  37091       46  1452     7     954
#>  7 0.062      1.55  1834    1834 Camd… 37029  37029       15   286     0     115
#>  8 0.091      1.28  1835    1835 Gates 37073  37073       37   420     0     254
#>  9 0.118      1.42  1836    1836 Warr… 37185  37185       93   968     4     748
#> 10 0.124      1.43  1837    1837 Stok… 37169  37169       85  1612     1     160
#> # … with 90 more rows, and 4 more variables: BIR79 <dbl>, SID79 <dbl>,
#> #   NWBIR79 <dbl>, geometry <MULTIPOLYGON [°]>
```

You can use any of the schemas to experiment by passing the `schema`
argument. These control how the geometry is stored in an Arrow data type
(but shouldn’t affect the round trip back to R).

``` r
write_geoarrow_parquet(nc, "nc.parquet", schema = geoarrow_schema_wkb())
sf::st_as_sf(read_geoarrow_parquet("nc.parquet"))
#> Simple feature collection with 100 features and 14 fields
#> Geometry type: MULTIPOLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -84.32385 ymin: 33.88199 xmax: -75.45698 ymax: 36.58965
#> CRS:           NA
#> # A tibble: 100 × 15
#>     AREA PERIMETER CNTY_ CNTY_ID NAME  FIPS  FIPSNO CRESS_ID BIR74 SID74 NWBIR74
#>    <dbl>     <dbl> <dbl>   <dbl> <chr> <chr>  <dbl>    <int> <dbl> <dbl>   <dbl>
#>  1 0.114      1.44  1825    1825 Ashe  37009  37009        5  1091     1      10
#>  2 0.061      1.23  1827    1827 Alle… 37005  37005        3   487     0      10
#>  3 0.143      1.63  1828    1828 Surry 37171  37171       86  3188     5     208
#>  4 0.07       2.97  1831    1831 Curr… 37053  37053       27   508     1     123
#>  5 0.153      2.21  1832    1832 Nort… 37131  37131       66  1421     9    1066
#>  6 0.097      1.67  1833    1833 Hert… 37091  37091       46  1452     7     954
#>  7 0.062      1.55  1834    1834 Camd… 37029  37029       15   286     0     115
#>  8 0.091      1.28  1835    1835 Gates 37073  37073       37   420     0     254
#>  9 0.118      1.42  1836    1836 Warr… 37185  37185       93   968     4     748
#> 10 0.124      1.43  1837    1837 Stok… 37169  37169       85  1612     1     160
#> # … with 90 more rows, and 4 more variables: BIR79 <dbl>, SID79 <dbl>,
#> #   NWBIR79 <dbl>, geometry <MULTIPOLYGON>
```

## Type examples

First, extension types for the kind of encodings existing libraries
support already:

``` r
geom_linestring <- wk::wkt("LINESTRING (1 2, 2 3)")

carrow::from_carrow_array(
  geoarrow_create(geom_linestring, schema = geoarrow_schema_wkb()),
  arrow::Array
)
#> Array
#> <fixed_size_binary[41]>
#> [
#>   010200000002000000000000000000F03F000000000000004000000000000000400000000000000840
#> ]

carrow::from_carrow_array(
  geoarrow_create(geom_linestring, schema = geoarrow_schema_wkt()),
  arrow::Array
)
#> Array
#> <string>
#> [
#>   "LINESTRING (1 2, 2 3)"
#> ]

carrow::from_carrow_array(
  geoarrow_create(geom_linestring, schema = geoarrow_schema_geojson()),
  arrow::Array
)
#> Array
#> <string>
#> [
#>   "{"type":"LineString","coordinates":[[1.0,2.0],[2.0,3.0]]}"
#> ]
```

…but also Arrow-native forms that don’t need to be parsed.

``` r
geom_point <- wk::wkt("POINT (1 2)")
geom_linestring <- wk::wkt("LINESTRING (1 2, 2 3)")
geom_poly <- wk::wkt("POLYGON ((0 0, 1 1, 0 1, 0 0))")
geom_multipoint <- wk::wkt("MULTIPOINT (1 2)")
geom_multilinestring <- wk::wkt("MULTILINESTRING ((1 2, 2 3))")
geom_multipoly <- wk::wkt("MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)))")

carrow::from_carrow_array(geoarrow_create(geom_point), arrow::Array)
#> FixedSizeListArray
#> <fixed_size_list<: double not null>[2]>
#> [
#>   [
#>     1,
#>     2
#>   ]
#> ]
carrow::from_carrow_array(geoarrow_create(geom_linestring), arrow::Array)
#> FixedSizeListArray
#> <fixed_size_list<: fixed_size_list<: double not null>[2] not null>[2]>
#> [
#>   [
#>     [
#>       1,
#>       2
#>     ],
#>     [
#>       2,
#>       3
#>     ]
#>   ]
#> ]
carrow::from_carrow_array(geoarrow_create(geom_poly), arrow::Array)
#> FixedSizeListArray
#> <fixed_size_list<: fixed_size_list<: fixed_size_list<: double not null>[2] not null>[4] not null>[1]>
#> [
#>   [
#>     [
#>       [
#>         0,
#>         0
#>       ],
#>       [
#>         1,
#>         1
#>       ],
#>       [
#>         0,
#>         1
#>       ],
#>       [
#>         0,
#>         0
#>       ]
#>     ]
#>   ]
#> ]
carrow::from_carrow_array(geoarrow_create(geom_multipoint), arrow::Array)
#> FixedSizeListArray
#> <fixed_size_list<: fixed_size_list<: double not null>[2] not null>[1]>
#> [
#>   [
#>     [
#>       1,
#>       2
#>     ]
#>   ]
#> ]
carrow::from_carrow_array(geoarrow_create(geom_multilinestring), arrow::Array)
#> FixedSizeListArray
#> <fixed_size_list<: fixed_size_list<: fixed_size_list<: double not null>[2] not null>[2]>[1]>
#> [
#>   [
#>     [
#>       [
#>         1,
#>         2
#>       ],
#>       [
#>         2,
#>         3
#>       ]
#>     ]
#>   ]
#> ]
carrow::from_carrow_array(geoarrow_create(geom_multipoly), arrow::Array)
#> FixedSizeListArray
#> <fixed_size_list<: fixed_size_list<: fixed_size_list<: fixed_size_list<: double not null>[2] not null>[4] not null>[1]>[1]>
#> [
#>   [
#>     [
#>       [
#>         [
#>           0,
#>           0
#>         ],
#>         [
#>           1,
#>           1
#>         ],
#>         [
#>           0,
#>           1
#>         ],
#>         [
#>           0,
#>           0
#>         ]
#>       ]
#>     ]
#>   ]
#> ]
```

Collections currently fall back on WKB but could theoretically be
supported using a union type:

``` r
geom_collection <- wk::wkt("GEOMETRYCOLLECTION (POINT (0 1), LINESTRING (1 1, 2 2))")
carrow::from_carrow_array(geoarrow_create(geom_collection), arrow::Array)
#> Array
#> <fixed_size_binary[71]>
#> [
#>   01070000000200000001010000000000000000000000000000000000F03F010200000002000000000000000000F03F000000000000F03F00000000000000400000000000000040
#> ]
```

The above examples all have a single feature/ring/linestring, so they
fall back to using a fixed-width list. The ability to use a fixed-width
list might be important if you have, say, 10 million rectangles that all
have 5 points where there’s no need to store a 64-bit offset to each
coordinate. For cases where there’s than `2 ^ 31` or more coordinates,
the offsets will need to be 64-bit integers (i.e., large_list type). You
can force either of these cases for any level of nesting by specifying a
schema by hand:

``` r
carrow::from_carrow_array(
  geoarrow_create(
    geom_poly,
    schema = geoarrow_schema_polygon(format = c("+L", "+l")),
    strict = TRUE
  ), 
  arrow::Array
)
#> LargeListArray
#> <large_list<: list<: fixed_size_list<: double not null>[2] not null> not null>>
#> [
#>   [
#>     [
#>       [
#>         0,
#>         0
#>       ],
#>       [
#>         1,
#>         1
#>       ],
#>       [
#>         0,
#>         1
#>       ],
#>       [
#>         0,
#>         0
#>       ]
#>     ]
#>   ]
#> ]
```

There are a few options for point storage. The default is to store
coordinates as a fixed-width list of doubles (float64) so that
coordinates stay together in memory. This is probably fastest and
mirrors the way that some other formats (e.g., WKB) store coordinates.

``` r
(points <- wk::xy(1:3, 4:6))
#> <wk_xy[3]>
#> [1] (1 4) (2 5) (3 6)
array <- geoarrow_create(points)
as.numeric(array$array_data$children[[1]]$buffers[[2]])
#> [1] 1 4 2 5 3 6
```

Alternatively, you can store coordinates as a struct. This is much like
storing x and y values in their own columns and might be useful if this
is already how a user has these values in memory (e.g., the user read in
a table where x and y values *were* their own columns). It also might be
faster for operations like adding and dropping dimensions because the
buffers stay separate for each dimension.

``` r
array <- geoarrow_create(points, schema = geoarrow_schema_point_struct())
as.numeric(array$array_data$children[[1]]$buffers[[2]])
#> [1] 1 2 3
as.numeric(array$array_data$children[[2]]$buffers[[2]])
#> [1] 4 5 6
```

For both of these, it might be adequate to store values as float32
instead of float64. I didn’t implement that here yet but in theory it
shouldn’t be a problem.

These are far from an exhaustive list of how coordinates can be stored.
For example, S2 cell IDs (64-bit integers with \~1 cm resolution
describing a location on the globe) are a fast and compact way to encode
geographic coordinates. The H3 library has a similar scheme where each
64-bit integer describes a hexagon on the sphere. Some libraries use
32-bit integers scaled by 1e6 to represent longitude and latitude, and
some provide an exact decimal type to skirt around floating point
precision issues, particularly for internal calculations (e.g.,
decimal128 or decimal256).

It might not be worth supporting all the point storage formats in any
particular implementation; however, the format is such that the
coordinate array can be swapped out independent of the rest of the array
structure.

## Why so many options?

The options are provided here so that implementations can experiment to
see if more than one is worth supporting.

## Metadata

The schemas here use column-level extension types and extension metadata
to encode dimension names, CRS information, and a flag to specify that
edges should be considered geodesic rather than Cartesian.

``` r
schema <- geoarrow_schema_linestring(
  geodesic = TRUE,
  point = geoarrow_schema_point(crs = "OGC:CRS84")
)
schema$metadata[["ARROW:extension:name"]]
#> [1] "geoarrow.linestring"
geoarrow_metadata(schema)
#> $geodesic
#> [1] "true"
geoarrow_metadata(schema$children[[1]])
#> $crs
#> [1] "OGC:CRS84"
#> 
#> $dim
#> [1] "xy"
```

I would argue that any string recognized by the latest PROJ release as a
CRS is valid for the `crs` item (which lives with the point array). This
includes full WKT2 output, which provides more detail at the expense of
including redundant information that can get out of sync. I think
“OGC:CRS84” or “EPSG:32620” are no less exact but are perhaps imprecise
in a way I don’t yet understand.
