
<!-- README.md is generated from README.Rmd. Please edit that file -->

# geoarrow

<!-- badges: start -->

[![Codecov test
coverage](https://codecov.io/gh/geoarrow/geoarrow-r/branch/main/graph/badge.svg)](https://app.codecov.io/gh/geoarrow/geoarrow-r?branch=main)
<!-- badges: end -->

The goal of geoarrow is to leverage the features of the
[arrow](https://arrow.apache.org/docs/r/) package and larger [Apache
Arrow](https://arrow.apache.org/) ecosystem for geospatial data. The
geoarrow package provides an R implementation of the
[GeoParquet](https://github.com/opengeospatial/geoparquet) file format
of and the draft [geoarrow data specification](https://geoarrow.org),
defining extension array types for vector geospatial data.

## Installation

You can install the released version of geoarrow from
[CRAN](https://cran.r-project.org/) with:

``` r
install.packages("nanoarrow")
```

You can install the development version of geoarrow from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("geoarrow/geoarrow-r")
```

## Example

The geoarrow package implements conversions to/from various geospatial
types (e.g., sf, sfc, s2, wk) with various Arrow representations (e.g.,
arrow, nanoarrow). The most useful conversions are between the **arrow**
and **sf** packages, which in most cases allow sf objects to be passed
to **arrow** functions directly after `library(geoarrow)` or
`requireNamespace("geoarrow")` has been called.

``` r
library(geoarrow)
library(arrow, warn.conflicts = FALSE)
#> Warning: package 'arrow' was built under R version 4.3.3
library(sf)
#> Linking to GEOS 3.11.0, GDAL 3.5.3, PROJ 9.1.0; sf_use_s2() is TRUE

nc <- read_sf(system.file("gpkg/nc.gpkg", package = "sf"))
tf <- tempfile(fileext = ".parquet")

nc |> 
  tibble::as_tibble() |> 
  write_parquet(tf)

open_dataset(tf) |> 
  dplyr::filter(startsWith(NAME, "A")) |>
  dplyr::select(NAME, geom) |> 
  st_as_sf()
#> Simple feature collection with 6 features and 1 field
#> Geometry type: MULTIPOLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -82.07776 ymin: 34.80792 xmax: -79.23799 ymax: 36.58965
#> Geodetic CRS:  NAD27
#>        NAME                           geom
#> 1      Ashe MULTIPOLYGON (((-81.47276 3...
#> 2 Alleghany MULTIPOLYGON (((-81.23989 3...
#> 3     Avery MULTIPOLYGON (((-81.94135 3...
#> 4  Alamance MULTIPOLYGON (((-79.24619 3...
#> 5 Alexander MULTIPOLYGON (((-81.10889 3...
#> 6     Anson MULTIPOLYGON (((-79.91995 3...
```

By default, arrow objects are converted to a neutral wrapper around
chunked Arrow memory, which in turn implements conversions to most
spatial types:

``` r
df <- read_parquet(tf)
df$geom
#> <geoarrow_vctr geoarrow.multipolygon{list}[100]>
#>  [1] <MULTIPOLYGON (((-81.4727554 36.2343559, -81.5408401 36.2725067, -81.56>
#>  [2] <MULTIPOLYGON (((-81.2398911 36.3653641, -81.2406921 36.3794174, -81.26>
#>  [3] <MULTIPOLYGON (((-80.4563446 36.2425575, -80.476387 36.2547264, -80.536>
#>  [4] <MULTIPOLYGON (((-76.0089722 36.3195953, -76.0173492 36.3377304, -76.03>
#>  [5] <MULTIPOLYGON (((-77.2176666 36.2409821, -77.2346115 36.2145996, -77.29>
#>  [6] <MULTIPOLYGON (((-76.7450638 36.2339172, -76.98069 36.2302361, -76.9947>
#>  [7] <MULTIPOLYGON (((-76.0089722 36.3195953, -75.9571838 36.1937714, -75.98>
#>  [8] <MULTIPOLYGON (((-76.5625076 36.3405685, -76.6042404 36.3149834, -76.64>
#>  [9] <MULTIPOLYGON (((-78.3087616 36.2600403, -78.2829285 36.2918816, -78.32>
#> [10] <MULTIPOLYGON (((-80.0256729 36.2502327, -80.4530106 36.2570877, -80.43>
#> [11] <MULTIPOLYGON (((-79.5305099 36.2461357, -79.5102997 36.547657, -79.217>
#> [12] <MULTIPOLYGON (((-79.5305099 36.2461357, -79.5305786 36.2361565, -80.02>
#> [13] <MULTIPOLYGON (((-78.7491226 36.063591, -78.788414 36.0621834, -78.8040>
#> [14] <MULTIPOLYGON (((-78.8068008 36.231575, -78.9510803 36.2338371, -79.159>
#> [15] <MULTIPOLYGON (((-78.4925232 36.1735878, -78.5147171 36.1752243, -78.51>
#> [16] <MULTIPOLYGON (((-77.3322067 36.0679817, -77.4053116 35.9947166, -77.42>
#> [17] <MULTIPOLYGON (((-76.2989273 36.2142296, -76.324234 36.2336235, -76.372>
#> [18] <MULTIPOLYGON (((-81.0205688 36.034935, -81.0840836 36.0207672, -81.124>
#> [19] <MULTIPOLYGON (((-81.806221 36.1045609, -81.8171539 36.1093864, -81.822>
#> [20] <MULTIPOLYGON (((-76.4805298 36.079792, -76.5369568 36.087925, -76.5755>
#> ...and 80 more values
st_as_sfc(df$geom)
#> Geometry set for 100 features 
#> Geometry type: MULTIPOLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -84.32385 ymin: 33.88199 xmax: -75.45698 ymax: 36.58965
#> Geodetic CRS:  NAD27
#> First 5 geometries:
#> MULTIPOLYGON (((-81.47276 36.23436, -81.54084 3...
#> MULTIPOLYGON (((-81.23989 36.36536, -81.24069 3...
#> MULTIPOLYGON (((-80.45634 36.24256, -80.47639 3...
#> MULTIPOLYGON (((-76.00897 36.3196, -76.01735 36...
#> MULTIPOLYGON (((-77.21767 36.24098, -77.23461 3...
```

The entry point to creating arrays is `as_geoarrow_vctr()`:

``` r
as_geoarrow_vctr(c("POINT (0 1)", "POINT (2 3)"))
#> <geoarrow_vctr geoarrow.wkt{string}[2]>
#> [1] <POINT (0 1)> <POINT (2 3)>
```

By default these do not attempt to create a new storage type; however,
you can request a storage type or infer one from the data:

``` r
as_geoarrow_vctr(c("POINT (0 1)", "POINT (2 3)"), schema = geoarrow_native("POINT"))
#> <geoarrow_vctr geoarrow.point{struct}[2]>
#> [1] <POINT (0 1)> <POINT (2 3)>

vctr <- as_geoarrow_vctr(c("POINT (0 1)", "POINT (2 3)"))
as_geoarrow_vctr(vctr, schema = infer_geoarrow_schema(vctr))
#> <geoarrow_vctr geoarrow.point{struct}[2]>
#> [1] <POINT (0 1)> <POINT (2 3)>
```

There are a number of files to use as examples at
<https://geoarrow.org/data> that can be read with
`arrow::read_ipc_file()`:

``` r
url <- "https://github.com/geoarrow/geoarrow-data/releases/download/v0.1.0/ns-water-basin_point.arrow"
tab <- read_ipc_file(url, as_data_frame = FALSE)
tab$geometry$type
#> GeometryExtensionType
#> geoarrow.multipoint <CRS: {"$schema":"https://proj.or...
```
