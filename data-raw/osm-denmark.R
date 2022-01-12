
library(arrow)
library(geoarrow)

# curl::curl_download(
#   "https://download.geofabrik.de/europe/denmark-latest-free.shp.zip",
#   "data-raw/denmark-latest-free.shp.zip"
# )
#
# dir.create("data-raw/denmark")
# unzip("data-raw/denmark-latest-free.shp.zip", exdir = "data-raw/denmark")

buildings_geom <- vapour::vapour_read_geometry("data-raw/denmark/gis_osm_buildings_a_free_1.shp")
buildings_attr <- vapour::vapour_read_attributes("data-raw/denmark/gis_osm_buildings_a_free_1.shp")

buildings_geom_wkb <- wk::new_wk_wkb(buildings_geom, crs = "OGC:CRS84", geodesic = TRUE)
bulidings_boundary_geom_wkb <- wk::as_wkb(geos::geos_boundary(buildings_geom_wkb)) |>
  wk::wk_set_geodesic(TRUE)

buildings_centroid <- s2::s2_centroid(buildings_geom_wkb)
buildings_xy <- wk::xy(
  s2::s2_x(buildings_centroid), s2::s2_y(buildings_centroid),
  crs = "OGC:CRS84"
)

buildings <- tibble::tibble(!!! buildings_attr, geometry = bulidings_boundary_geom_wkb)
buildings_sf <- tibble::tibble(
  !!! buildings_attr,
  geometry = wk::wk_translate(bulidings_boundary_geom_wkb, sf::st_sfc(crs = "OGC:CRS84"))
)

write_geoarrow_parquet(
  buildings,
  "data-raw/denmark-buildings-point.parquet",
  schema = geoarrow_schema_linestring(
    point = geoarrow_schema_point(
      crs = wk::wk_crs(buildings),
      nullable = TRUE
    )
  )
)

write_geoarrow_parquet(
  buildings,
  "data-raw/denmark-buildings-point-struct.parquet",
  schema = geoarrow_schema_linestring(
    point = geoarrow_schema_point_struct(
      crs = wk::wk_crs(buildings),
      nullable = TRUE
    )
  )
)

buildings_tbl <- arrow::read_parquet("data-raw/denmark-buildings-point.parquet", as_data_frame = FALSE)
buildings_tbl_struct <- arrow::read_parquet("data-raw/denmark-buildings-point-struct.parquet", as_data_frame = FALSE)

# about the same for points (reading)
bench::mark(
  point = arrow::read_parquet("data-raw/denmark-buildings-point.parquet", as_data_frame = FALSE),
  point_struct = arrow::read_parquet("data-raw/denmark-buildings-point-struct.parquet", as_data_frame = FALSE),
  check = FALSE
)

buildings_array <- carrow::as_carrow_array(buildings_tbl$geometry$chunk(0))
buildings_array$schema <- geoarrow_schema_linestring(
  point = geoarrow_schema_point(
    nullable = TRUE
  )
)
buildings_array_struct <- carrow::as_carrow_array(buildings_tbl_struct$geometry$chunk(0))
buildings_array_struct$schema <- geoarrow_schema_linestring(
  point = geoarrow_schema_point_struct(
    nullable = TRUE
  )
)

bench::mark(
  point = wk::wk_void(buildings_array),
  point_struct = wk::wk_void(buildings_array_struct)
)
