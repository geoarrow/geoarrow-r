
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
read_geoarrow_parquet_sf("nc.parquet")
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
read_geoarrow_parquet_sf("nc.parquet")
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
