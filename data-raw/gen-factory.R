
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
case GeoArrowMeta::StorageType::List:
    ${ switch_child("ListView<%s, int32_t>", indent) }
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


factory <- glue("
// autogen factory start
GeoArrowArrayView* create_view_point(struct ArrowSchema* schema, GeoArrowMeta& point_meta) {
${point_switch()}
}

GeoArrowArrayView* create_view_linestring(struct ArrowSchema* schema,
                                          GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]);

${linestring_switch()}
}

GeoArrowArrayView* create_view_polygon(struct ArrowSchema* schema, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]);

${polygon_switch()}
}

GeoArrowArrayView* create_view_multipoint(struct ArrowSchema* schema,
                                          GeoArrowMeta& multi_meta, GeoArrowMeta& point_meta) {


${multi_switch(point_switch)}
}

GeoArrowArrayView* create_view_multilinestring(struct ArrowSchema* schema,
                                               GeoArrowMeta& multi_meta,
                                               GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]->children[0]);

${multi_switch(linestring_switch)}
}

GeoArrowArrayView* create_view_multipolygon(struct ArrowSchema* schema,
                                            GeoArrowMeta& multi_meta, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]->children[0]);

${multi_switch(polygon_switch)}
}
// autogen factory end
")


current <- readr::read_file("src/internal/geoarrow-factory.hpp")
new <- stringr::str_replace(
  current,
  stringr::regex(
    "// autogen factory start.*?// autogen factory end",
    multiline = TRUE, dotall = TRUE
  ),
  factory
)
readr::write_file(new, "src/internal/geoarrow-factory.hpp")


clipr::write_clip(point_switch())
clipr::write_clip(linestring_switch())
clipr::write_clip(polygon_switch())

clipr::write_clip(multi_switch(point_switch))
clipr::write_clip(multi_switch(linestring_switch))
clipr::write_clip(multi_switch(polygon_switch))
