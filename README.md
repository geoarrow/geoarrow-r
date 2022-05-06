
<!-- README.md is generated from README.Rmd. Please edit that file -->

# geoarrow

<!-- badges: start -->

[![R-CMD-check](https://github.com/paleolimbot/geoarrow/workflows/R-CMD-check/badge.svg)](https://github.com/paleolimbot/geoarrow/actions)
[![Codecov test
coverage](https://codecov.io/gh/paleolimbot/geoarrow/branch/master/graph/badge.svg)](https://codecov.io/gh/paleolimbot/geoarrow?branch=master)
<!-- badges: end -->

The goal of geoarrow is to leverage the features of the
[arrow](https://arrow.apache.org/docs/r/) package and larger [Apache
Arrow](https://arrow.apache.org/) ecosystem for geospatial data. The
geoarrow package provides an R implementation of the draft [geoarrow
data specification](https://github.com/geopandas/geo-arrow-spec),
defining extension array types for vector geospatial data.

## Installation

You can install the development version from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("paleolimbot/geoarrow")
```

## Write geospatial data to Parquet

Parquet is a compact binary file format that enables fast reading and
efficient compression. You can write geospatial data (e.g., sf objects)
to Parquet using `geoarrow_write_parquet()` and read them using
`geoarrow_read_parquet()`.

``` r
library(geoarrow)

nc <- sf::read_sf(system.file("shape/nc.shp", package = "sf"))
write_geoparquet(nc, "nc.parquet")
read_geoparquet_sf("nc.parquet")
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

You can also use `arrow::open_dataset()` and `geoarrow_collect_sf()` to
use the full power of the Arrow compute engine on datasets of one or
more files:

``` r
library(arrow)
library(dplyr)

(query <- open_dataset("nc.parquet") %>%
  filter(grepl("^A", NAME)) %>%
  select(NAME, geometry) )
#> FileSystemDataset (query)
#> NAME: string
#> geometry: list<polygons: list<rings: list<vertices: fixed_size_list<xy: double>[2]>>>
#>
#> * Filter: match_substring_regex(NAME, {pattern="^A", ignore_case=false})
#> See $.data for the source Arrow object

query %>%
  geoarrow_collect_sf()
#> Simple feature collection with 6 features and 1 field
#> Geometry type: MULTIPOLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -82.07776 ymin: 34.80792 xmax: -79.23799 ymax: 36.58965
#> Geodetic CRS:  NAD27
#> # A tibble: 6 × 2
#>   NAME                                                                  geometry
#>   <chr>                                                       <MULTIPOLYGON [°]>
#> 1 Ashe      (((-81.47276 36.23436, -81.54084 36.27251, -81.56198 36.27359, -81.…
#> 2 Alleghany (((-81.23989 36.36536, -81.24069 36.37942, -81.26284 36.40504, -81.…
#> 3 Avery     (((-81.94135 35.95498, -81.9614 35.93922, -81.94495 35.91861, -81.9…
#> 4 Alamance  (((-79.24619 35.86815, -79.23799 35.83725, -79.54099 35.83699, -79.…
#> 5 Alexander (((-81.10889 35.7719, -81.12728 35.78897, -81.1414 35.82332, -81.32…
#> 6 Anson     (((-79.91995 34.80792, -80.32528 34.81476, -80.27512 35.19311, -80.…
```
