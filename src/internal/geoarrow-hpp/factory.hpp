
#pragma once

#include "array-view-geoarrow.hpp"
#include "array-view-wkb.hpp"
#include "array-view-wkt.hpp"

namespace geoarrow {

namespace {

ArrayView* create_view_collection(struct ArrowSchema* schema, Meta& multi_meta) {
    Meta child_meta(schema->children[0]);

    switch (child_meta.extension_) {
    case util::Extension::Point:
        return new CollectionArrayView<PointArrayView>(schema);

    case util::Extension::Linestring:
        return new CollectionArrayView<LinestringArrayView>(schema);

    case util::Extension::Polygon:
        return new CollectionArrayView<PolygonArrayView>(schema);
    default:
        throw Meta::ValidationError(
            "Unsupported extension type for child of geoarrow.geometrycollection");
    }
}

ArrayView* create_view_wkb(struct ArrowSchema* schema, Meta& geoarrow_meta) {
    switch (geoarrow_meta.storage_type_) {
    case util::StorageType::Binary:
        return new WKBArrayView(schema);
    case util::StorageType::LargeBinary:
        return new LargeWKBArrayView(schema);
    case util::StorageType::FixedWidthBinary:
        return new FixedWidthWKBArrayView(schema);
    default:
        throw Meta::ValidationError(
            "Unsupported storage type for extension geoarrow.wkb");
    }
}

ArrayView* create_view_wkt(struct ArrowSchema* schema, Meta& geoarrow_meta) {
    switch (geoarrow_meta.storage_type_) {
    case util::StorageType::Binary:
    case util::StorageType::String:
        return new WKTArrayView(schema);
    case util::StorageType::LargeBinary:
    case util::StorageType::LargeString:
        return new LargeWKTArrayView(schema);
    default:
        throw Meta::ValidationError(
            "Unsupported storage type for extension geoarrow.wkt");
    }
}

} // anonymous namespace

ArrayView* create_view(struct ArrowSchema* schema) {
    // parse the schema and check that the structure is not unexpected
    // (e.g., the extension type and storage type are compatible and
    // there are not an unexpected number of children)
    Meta geoarrow_meta(schema);

    switch (geoarrow_meta.extension_) {
    case util::Extension::Point:
        return new PointArrayView(schema);

    case util::Extension::Linestring:
        return new LinestringArrayView(schema);

    case util::Extension::Polygon:
        return new PolygonArrayView(schema);

    case util::Extension::MultiPoint:
    case util::Extension::MultiLinestring:
    case util::Extension::MultiPolygon:
    case util::Extension::GeometryCollection:
        return create_view_collection(schema, geoarrow_meta);

    case util::Extension::WKB:
        return create_view_wkb(schema, geoarrow_meta);

    case util::Extension::WKT:
        return create_view_wkt(schema, geoarrow_meta);

    default:
        throw Meta::ValidationError("Unsupported extension type");
    }
}

}
