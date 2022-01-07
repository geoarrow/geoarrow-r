
#include "geoarrow-native.hpp"

namespace geoarrow {

GeoArrowArrayView* create_view_point(struct ArrowSchema* schema, GeoArrowMeta& geoarrow_meta) {
    switch (geoarrow_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:
        return new GeoArrowPointView(schema);
    
    case GeoArrowMeta::StorageType::Struct:
        return new GeoArrowPointStructView(schema);
    
    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.point");
    }    
}

GeoArrowArrayView* create_view_linestring(struct ArrowSchema* schema, GeoArrowMeta& geoarrow_meta) {
    switch (geoarrow_meta.storage_type_) {
    // case GeoArrowMeta::StorageType::FixedWidthList:
    //     break;
    
    // case GeoArrowMeta::StorageType::List:
    //     break;
    
    // case GeoArrowMeta::StorageType::LargeList:
    //     break;
    
    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.linestring");
    }
}

GeoArrowArrayView* create_view_polygon(struct ArrowSchema* schema, GeoArrowMeta& geoarrow_meta) {
    switch (geoarrow_meta.storage_type_) {
    // case GeoArrowMeta::StorageType::FixedWidthList:
    //     break;
    
    // case GeoArrowMeta::StorageType::List:
    //     break;
    
    // case GeoArrowMeta::StorageType::LargeList:
    //     break;
    
    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.multi");
    }
}

GeoArrowArrayView* create_view_multi(struct ArrowSchema* schema, GeoArrowMeta& geoarrow_meta) {
    switch (geoarrow_meta.storage_type_) {
    // case GeoArrowMeta::StorageType::FixedWidthList:
    //     break;
    
    // case GeoArrowMeta::StorageType::List:
    //     break;
    
    // case GeoArrowMeta::StorageType::LargeList:
    //     break;
    
    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.multi");
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
