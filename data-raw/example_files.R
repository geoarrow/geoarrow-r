
library(geoarrow)

# add attributes to nc
nc_tbl <- sf::st_set_geometry(
  sf::read_sf(system.file("shape/nc.shp", package = "sf")),
  NULL
)
nc_tbl <- tibble::as_tibble(nc_tbl)
nc_tbl$geometry <- geoarrow_example_wkt$nc
nc_tbl_spherical <- nc_tbl
wk::wk_is_geodesic(nc_tbl_spherical) <- TRUE


# make tibbles for each example
simple_example_names <- setdiff(names(geoarrow_example_wkt), "nc")
example_tbl <- lapply(
  geoarrow_example_wkt[simple_example_names],
  function(ex) {
    tibble::tibble(row_num = seq_along(ex), geometry = ex)
  }
)

example_tbl$nc <- nc_tbl
example_tbl$nc_spherical <- nc_tbl_spherical

# write files with multiple encodings
for (name in names(example_tbl)) {
  # ipc
  write_geoparquet_ipc_stream(
    example_tbl[[name]],
    glue::glue("inst/example_ipc_stream/{name}-default.arrows"),
    compression = "uncompressed",
    schema = geoarrow_schema_default(
      example_tbl[[name]],
      point = geoarrow_schema_point()
    )
  )

  write_geoparquet_ipc_stream(
    example_tbl[[name]],
    glue::glue("inst/example_ipc_stream/{name}-wkt.arrows"),
    compression = "uncompressed",
    schema = geoarrow_schema_wkt()
  )

  write_geoparquet_ipc_stream(
    example_tbl[[name]],
    glue::glue("inst/example_ipc_stream/{name}-wkb.arrows"),
    compression = "uncompressed",
    schema = geoarrow_schema_wkb()
  )

  # feather
  write_geoparquet_feather(
    example_tbl[[name]],
    glue::glue("inst/example_feather/{name}-default.feather"),
    compression = "uncompressed",
    schema = geoarrow_schema_default(
      example_tbl[[name]],
      point = geoarrow_schema_point()
    )
  )

  write_geoparquet_feather(
    example_tbl[[name]],
    glue::glue("inst/example_feather/{name}-wkt.feather"),
    compression = "uncompressed",
    schema = geoarrow_schema_wkt()
  )

  write_geoparquet_feather(
    example_tbl[[name]],
    glue::glue("inst/example_feather/{name}-wkb.feather"),
    compression = "uncompressed",
    schema = geoarrow_schema_wkb()
  )

  # parquet
  write_geoparquet(
    example_tbl[[name]],
    glue::glue("inst/example_parquet/{name}-default.parquet"),
    compression = "uncompressed",
    schema = geoarrow_schema_default(
      example_tbl[[name]],
      point = geoarrow_schema_point()
    )
  )

  write_geoparquet(
    example_tbl[[name]],
    glue::glue("inst/example_parquet/{name}-wkt.parquet"),
    compression = "uncompressed",
    schema = geoarrow_schema_wkt()
  )

  write_geoparquet(
    example_tbl[[name]],
    glue::glue("inst/example_parquet/{name}-wkb.parquet"),
    compression = "uncompressed",
    schema = geoarrow_schema_wkb()
  )
}
