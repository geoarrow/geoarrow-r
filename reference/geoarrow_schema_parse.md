# Inspect a GeoArrow schema

Inspect a GeoArrow schema

## Usage

``` r
geoarrow_schema_parse(
  schema,
  extension_name = NULL,
  infer_from_storage = FALSE
)

is_geoarrow_schema(schema)

as_geoarrow_schema(schema)
```

## Arguments

- schema:

  A
  [nanoarrow_schema](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_schema.html)

- extension_name:

  An extension name to use if schema is a storage type.

- infer_from_storage:

  Attempt to guess an extension name if schema is not a geoarrow
  extension type.

## Value

A list of parsed properties

## Examples

``` r
geoarrow_schema_parse(na_extension_geoarrow("POINT"))
#> $id
#> [1] 1
#> 
#> $geometry_type
#> [1] 1
#> 
#> $dimensions
#> [1] 1
#> 
#> $coord_type
#> [1] 1
#> 
#> $extension_name
#> [1] "geoarrow.point"
#> 
#> $crs_type
#> [1] 0
#> 
#> $crs
#> [1] ""
#> 
#> $edge_type
#> [1] 0
#> 
```
