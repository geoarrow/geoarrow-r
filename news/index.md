# Changelog

## geoarrow 0.4.1

CRAN release: 2025-11-19

- Fix test for updated sf dependency
  ([\#76](https://github.com/geoarrow/geoarrow-r/issues/76)).

## geoarrow 0.4.0

CRAN release: 2025-09-21

- When converting sf objects to geoarrow, an optional geometry vctr type
  can now be specified (e.g., to force geoarrow.wkb output)
  ([\#71](https://github.com/geoarrow/geoarrow-r/issues/71)).
- The internal vendored copies of geoarrow-c and nanoarrow were updated
  ([\#72](https://github.com/geoarrow/geoarrow-r/issues/72)).

## geoarrow 0.3.0

CRAN release: 2025-05-26

- Add support for the `geoarrow.box` type with conversions to and from
  [`wk::rct()`](https://paleolimbot.github.io/wk/reference/rct.html)
  ([\#59](https://github.com/geoarrow/geoarrow-r/issues/59)).
- Implement
  [`wk::wk_set_crs()`](https://paleolimbot.github.io/wk/reference/wk_crs.html)
  and
  [`wk::wk_set_geodesic()`](https://paleolimbot.github.io/wk/reference/wk_is_geodesic.html)
  for the `geoarrow_vctr` class
  ([\#61](https://github.com/geoarrow/geoarrow-r/issues/61)).
- Fix conversion of empty
  [`sf::st_crs()`](https://r-spatial.github.io/sf/reference/st_crs.html)
  to GeoArrow CRS
  ([\#60](https://github.com/geoarrow/geoarrow-r/issues/60)).
- The `geoarrow_vctr` can now be rearranged and subset with a non-slice
  range when the arrow package is installed
  ([\#62](https://github.com/geoarrow/geoarrow-r/issues/62)).
- The R package now supports extra metadata features that were
  introduced in the GeoArrow 0.2 specification, including non-PROJJSON
  CRS types and spheroidal edge interpolations
  ([\#57](https://github.com/geoarrow/geoarrow-r/issues/57),
  [\#65](https://github.com/geoarrow/geoarrow-r/issues/65)).
- `geoarrow.wkb` and `geoarrow.wkt` extension types now support binary
  view and string view storage types, respectively
  ([\#66](https://github.com/geoarrow/geoarrow-r/issues/66)).
- Internal versions of geoarrow-c, nanoarrow, and fast_float were
  updated ([\#64](https://github.com/geoarrow/geoarrow-r/issues/64)).

## geoarrow 0.2.1

CRAN release: 2024-06-13

- Fix undefined behaviour identified by UBSAN
  ([\#44](https://github.com/geoarrow/geoarrow-r/issues/44)).

## geoarrow 0.2.0

CRAN release: 2024-05-31

- Initial CRAN submission.
