
glue <- function(..., .envir = parent.frame(), .indent = "") {
  result <- glue::glue(..., .open = "${", .close = "}", .envir = .envir)
  glue::as_glue(gsub("\n", paste0("\n", .indent), result))
}

point_switch_template <- '

switch (point_meta.storage_type_) {
case GeoArrowMeta::StorageType::FixedWidthList:
    return new ${ type("GeoArrowPointView") }(schema);

case GeoArrowMeta::StorageType::Struct:
    return new ${ type("GeoArrowPointStructView") }(schema);

default:
    throw GeoArrowMeta::ValidationError(
        "Unsupported storage type for extension geoarrow.point");
}

'

nest_switch_template <- '

switch (${ extension }_meta.storage_type_) {
case GeoArrowMeta::StorageType::FixedWidthList:
    ${ switch_child("FixedWidthListView<%s>", indent) }
    break;

case GeoArrowMeta::StorageType::List:
    ${ switch_child("ListView<%s, int32_t>", indent) }
    break;

case GeoArrowMeta::StorageType::LargeList:
    ${ switch_child("ListView<%s, int64_t>", indent) }
    break;

default:
    throw GeoArrowMeta::ValidationError(
        "Unsupported storage type for extension geoarrow.${ extension }");
}

'

point_switch <- function(type = identity, indent = "    ") {
  glue(point_switch_template, .indent = indent)
}

linestring_switch <- function(indent = "    ", extension = "linestring",
                              name = "GeoArrowLinestringView",
                              child_templ = function(type_format, point_type) sprintf(type_format, point_type)) {
  switch_child <- function(type_format, indent) {
    type <- function(point_type) {
      glue("${ name }<${ point_type }, ${ child_templ(type_format, point_type) }>")
    }

    point_switch(type, indent = paste0(indent, "    "))
  }

  glue(nest_switch_template, .indent = indent)
}

polygon_switch <- function(indent = "    ", extension = "polygon") {
  switch_child <- function(type_format, indent) {
    linestring_switch(
      indent = paste0(indent, "    "),
      child_templ = function(linestring_type_format, point_type) {
        paste(
          sprintf(linestring_type_format, point_type),
          sprintf(type_format, sprintf(linestring_type_format, point_type)),
          sep = ", "
        )
      },
      name = "GeoArrowPolygonView"
    )
  }

  glue(nest_switch_template, .indent = indent)
}

multi_switch <- function(type, indent = "    ", extension = "multi") {
  switch_child <- function(type_format, indent) {
    type <- function(child_type) {
      glue("GeoArrowMultiView<${ child_type }, ${ sprintf(type_format, child_type) }>")
    }

    point_switch(type, indent = paste0(indent, "    "))
  }

  glue(nest_switch_template, .indent = indent)
}

multi_switch <- function(switcher, indent = "    ", extension = "multi") {
  switch_child <- function(type_format, indent) {
    child <- switcher(indent = "")
    format <- gsub(
      "new (.*?)\\(",
      "new GeoArrowMultiView<\\1, ${ sprintf(type_format, '\\1') }>(",
      child
    )
    glue(format, indent = paste0(indent, "    "))
  }

  glue(nest_switch_template, .indent = indent)
}

clipr::write_clip(point_switch())
clipr::write_clip(linestring_switch())
clipr::write_clip(polygon_switch())

clipr::write_clip(multi_switch(point_switch))
clipr::write_clip(multi_switch(linestring_switch))
clipr::write_clip(multi_switch(polygon_switch))
