
library(tidyverse)
library(arrow)
library(geoarrow)

if (!dir.exists("data-raw/denmark")) {
  curl::curl_download(
    "https://download.geofabrik.de/europe/denmark-latest-free.shp.zip",
    "data-raw/denmark-latest-free.shp.zip"
  )

  dir.create("data-raw/denmark")
  unzip("data-raw/denmark-latest-free.shp.zip", exdir = "data-raw/denmark")
}

files <- tibble(
  shp = list.files("data-raw/denmark", ".shp$", full.names = TRUE)
) %>%
  extract(shp, "name", "gis_(.*?)_free", remove = FALSE) %>%
  mutate(
    content = map(shp, sf::read_sf)
  )

if (!dir.exists("data-raw/denmark_osm")) {
  dir.create("data-raw/denmark_osm")
}

dst <- "data-raw/denmark_osm"

for (name in files$name) {
  message(glue::glue("Processing '{ name }'"))

  src <- files$content[[match(name, files$name)]]
  wk::wk_crs(src) <- wk::wk_crs_longlat()

  # some common existing formats
  sf::write_sf(src, glue::glue("{dst}/{name}.gpkg"))
  sf::write_sf(src, glue::glue("{dst}/{name}.fgb"))

  # for verification
  src_wkb <- wk::wk_handle(src, wk::wkb_writer())
  src_crs <- wk::wk_crs_proj_definition(sf::st_crs(src), verbose = TRUE)
  attributes(src_wkb) <- NULL

  check_output <- function(file) {
    check <- read_geoarrow_parquet(
      file,
      handler = wk::wkb_writer()
    )

    if (!identical(wk::wk_crs(check$geometry), src_crs)) {
      message(glue::glue("CRS was not identical for file {file}"))
      print(waldo::compare(wk::wk_crs(check$geometry), src_crs))
    }

    attributes(check$geometry) <- NULL
    if (!identical(check$geometry, src_wkb)) {
      message(glue::glue("Geometry not identical for file {file}"))
    }
  }

  write_geoarrow_parquet(
    src,
    glue::glue("{dst}/{name}-wkb.parquet"),
    compression = "uncompressed",
    schema = geoarrow_schema_wkb()
  )
  check_output(glue::glue("{dst}/{name}-wkb.parquet"))

  write_geoarrow_parquet(
    src,
    glue::glue("{dst}/{name}-geoarrow.parquet"),
    compression = "uncompressed"
  )
  check_output(glue::glue("{dst}/{name}-geoarrow.parquet"))
}

# copy a few to a test dataset folder in the inst/ folder
unlink("inst/denmark_osm", recursive = TRUE)
dir.create("inst/denmark_osm")

arrow::open_dataset("data-raw/denmark_osm/osm_places-geoarrow.parquet") %>%
  dplyr::group_by(fclass) %>%
  arrow::write_dataset("inst/example_dataset/osm_places")
