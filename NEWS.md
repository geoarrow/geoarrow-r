# geoarrow 0.3.0

* Add support for the `geoarrow.box` type with conversions to
  and from `wk::rct()` (#59).
* Implement `wk::wk_set_crs()` and `wk::wk_set_geodesic()` for the
  `geoarrow_vctr` class (#61).
* Fix conversion of empty `sf::st_crs()` to GeoArrow CRS (#60).
* The `geoarrow_vctr` can now be rearranged and subset with a non-slice
  range when the arrow package is installed (#62).
* The R package now supports extra metadata features that were introduced
  in the GeoArrow 0.2 specification, including non-PROJJSON CRS types
  and spheroidal edge interpolations (#57, #65).
* `geoarrow.wkb` and `geoarrow.wkt` extension types now support
  binary view and string view storage types, respectively (#66).
* Internal versions of geoarrow-c, nanoarrow, and fast_float were updated
  (#64).

# geoarrow 0.2.1

* Fix undefined behaviour identified by UBSAN (#44).

# geoarrow 0.2.0

* Initial CRAN submission.
