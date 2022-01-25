
#include "geoarrow-native.hpp"

namespace geoarrow {

// autogen factory start
GeoArrowArrayView* create_view_point(struct ArrowSchema* schema, GeoArrowMeta& point_meta) {

}

GeoArrowArrayView* create_view_linestring(struct ArrowSchema* schema, GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]);


}

GeoArrowArrayView* create_view_polygon(struct ArrowSchema* schema, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]);


}

GeoArrowArrayView* create_view_multipoint(struct ArrowSchema* schema,
                                          GeoArrowMeta& multi_meta, GeoArrowMeta& point_meta) {



}

GeoArrowArrayView* create_view_multilinestring(struct ArrowSchema* schema,
                                               GeoArrowMeta& multi_meta, GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]->children[0]);


}

GeoArrowArrayView* create_view_multipolygon(struct ArrowSchema* schema,
                                            GeoArrowMeta& multi_meta, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]->children[0]);

}
// autogen factory end


GeoArrowArrayView* create_view_multi(struct ArrowSchema* schema, GeoArrowMeta& multi_meta) {
    GeoArrowMeta child_meta(schema->children[0]);

    switch (child_meta.extension_) {
    case GeoArrowMeta::Extension::Point:
        return create_view_multipoint(schema, multi_meta, child_meta);

    case GeoArrowMeta::Extension::Linestring:
        return create_view_multilinestring(schema, multi_meta, child_meta);

    case GeoArrowMeta::Extension::Polygon:
        return create_view_multipolygon(schema, multi_meta, child_meta);
    default:
        throw GeoArrowMeta::ValidationError("Unsupported extension type for child of geoarrow.multi");
    }
}


GeoArrowArrayView* create_view(struct ArrowSchema* schema) {
    // parse the schema and check that the structure is not unexpected
    // (e.g., the extension type and storage type are compatible and
    // there are not an unexpected number of children)
    GeoArrowMeta geoarrow_meta(schema);

    switch (geoarrow_meta.extension_) {
    case GeoArrowMeta::Extension::Point:
        return create_view_point(schema, geoarrow_meta);

    case GeoArrowMeta::Extension::Linestring:
        return create_view_linestring(schema, geoarrow_meta);

    case GeoArrowMeta::Extension::Polygon:
        return create_view_polygon(schema, geoarrow_meta);

    case GeoArrowMeta::Extension::Multi:
        return create_view_multi(schema, geoarrow_meta);

    default:
        throw GeoArrowMeta::ValidationError("Unsupported extension type");
    }
}

}; // namespace geoarrow
