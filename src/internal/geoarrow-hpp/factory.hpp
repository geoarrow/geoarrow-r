
#pragma once

#include "view.hpp"

namespace geoarrow {


namespace {

// autogen factory start
ArrayView* create_view_point(struct ArrowSchema* schema, GeoArrowMeta& point_meta) {

    switch (point_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:
        return new PointArrayView(schema);

    case GeoArrowMeta::StorageType::Struct:
        return new GeoArrowPointStructView(schema);

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.point");
    }

}

ArrayView* create_view_linestring(struct ArrowSchema* schema,
                                          GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]);


    switch (linestring_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:

            switch (point_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:
                return new LinestringArrayView<PointArrayView>(schema);

            case GeoArrowMeta::StorageType::Struct:
                return new LinestringArrayView<GeoArrowPointStructView>(schema);

            default:
                throw GeoArrowMeta::ValidationError(
                    "Unsupported storage type for extension geoarrow.point");
            }

        break;

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.linestring");
    }

}

ArrayView* create_view_polygon(struct ArrowSchema* schema, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]);


    switch (polygon_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:

            switch (linestring_meta.storage_type_) {
            case GeoArrowMeta::StorageType::List:

                        switch (point_meta.storage_type_) {
                        case GeoArrowMeta::StorageType::FixedWidthList:
                            return new PolygonArrayView<PointArrayView>(schema);

                        case GeoArrowMeta::StorageType::Struct:
                            return new PolygonArrayView<GeoArrowPointStructView>(schema);

                        default:
                            throw GeoArrowMeta::ValidationError(
                                "Unsupported storage type for extension geoarrow.point");
                        }

                break;

            default:
                throw GeoArrowMeta::ValidationError(
                    "Unsupported storage type for extension geoarrow.linestring");
            }

        break;

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.polygon");
    }

}

ArrayView* create_view_multipoint(struct ArrowSchema* schema,
                                          GeoArrowMeta& multi_meta, GeoArrowMeta& point_meta) {



    switch (multi_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:
        switch (point_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:
        return new CollectionArrayView<PointArrayView>(schema);

    case GeoArrowMeta::StorageType::Struct:
        return new CollectionArrayView<GeoArrowPointStructView>(schema);

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.point");
    }
        break;

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.multi");
    }

}

ArrayView* create_view_multilinestring(struct ArrowSchema* schema,
                                               GeoArrowMeta& multi_meta,
                                               GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]->children[0]);


    switch (multi_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:
        switch (linestring_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:

        switch (point_meta.storage_type_) {
        case GeoArrowMeta::StorageType::FixedWidthList:
            return new CollectionArrayView<LinestringArrayView<PointArrayView>>(schema);

        case GeoArrowMeta::StorageType::Struct:
            return new CollectionArrayView<LinestringArrayView<GeoArrowPointStructView>>(schema);

        default:
            throw GeoArrowMeta::ValidationError(
                "Unsupported storage type for extension geoarrow.point");
        }

        break;

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.linestring");
    }
        break;

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.multi");
    }

}

ArrayView* create_view_multipolygon(struct ArrowSchema* schema,
                                            GeoArrowMeta& multi_meta, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]->children[0]);


    switch (multi_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:
        switch (polygon_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:

        switch (linestring_meta.storage_type_) {
        case GeoArrowMeta::StorageType::List:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new CollectionArrayView<PolygonArrayView<PointArrayView>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new CollectionArrayView<PolygonArrayView<GeoArrowPointStructView>>(schema);

                default:
                    throw GeoArrowMeta::ValidationError(
                        "Unsupported storage type for extension geoarrow.point");
                }

            break;

        default:
            throw GeoArrowMeta::ValidationError(
                "Unsupported storage type for extension geoarrow.linestring");
        }

        break;

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.polygon");
    }
        break;

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.multi");
    }

}
// autogen factory end


ArrayView* create_view_multi(struct ArrowSchema* schema, GeoArrowMeta& multi_meta) {
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

} // anonymous namespace

ArrayView* create_view(struct ArrowSchema* schema) {
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

}
