
#include "geoarrow-native.hpp"

namespace geoarrow {

GeoArrowArrayView* create_view_point(struct ArrowSchema* schema, GeoArrowMeta& point_meta) {
    switch (point_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:
        return new GeoArrowPointView(schema);

    case GeoArrowMeta::StorageType::Struct:
        return new GeoArrowPointStructView(schema);

    default:
        throw GeoArrowMeta::ValidationError(
            "Unsupported storage type for extension geoarrow.point");
    }
}

GeoArrowArrayView* create_view_linestring(struct ArrowSchema* schema, GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]);

    switch (linestring_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:

        switch (point_meta.storage_type_) {
        case GeoArrowMeta::StorageType::FixedWidthList:
            return new GeoArrowLinestringView<GeoArrowPointView, FixedWidthListView<GeoArrowPointView>>(schema);

        case GeoArrowMeta::StorageType::Struct:
            return new GeoArrowLinestringView<GeoArrowPointStructView, FixedWidthListView<GeoArrowPointStructView>>(schema);

        default:
            throw GeoArrowMeta::ValidationError(
                "Unsupported storage type for extension geoarrow.point");
        }

        break;

    case GeoArrowMeta::StorageType::List:

        switch (point_meta.storage_type_) {
        case GeoArrowMeta::StorageType::FixedWidthList:
            return new GeoArrowLinestringView<GeoArrowPointView, ListView<GeoArrowPointView, int32_t>>(schema);

        case GeoArrowMeta::StorageType::Struct:
            return new GeoArrowLinestringView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int32_t>>(schema);

        default:
            throw GeoArrowMeta::ValidationError(
                "Unsupported storage type for extension geoarrow.point");
        }

        break;

    case GeoArrowMeta::StorageType::LargeList:

        switch (point_meta.storage_type_) {
        case GeoArrowMeta::StorageType::FixedWidthList:
            return new GeoArrowLinestringView<GeoArrowPointView, ListView<GeoArrowPointView, int64_t>>(schema);

        case GeoArrowMeta::StorageType::Struct:
            return new GeoArrowLinestringView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int64_t>>(schema);

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

GeoArrowArrayView* create_view_polygon(struct ArrowSchema* schema, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]);

    switch (polygon_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:

            switch (linestring_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, FixedWidthListView<GeoArrowPointView>, FixedWidthListView<FixedWidthListView<GeoArrowPointView>>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, FixedWidthListView<GeoArrowPointStructView>, FixedWidthListView<FixedWidthListView<GeoArrowPointStructView>>>(schema);

                default:
                    throw GeoArrowMeta::ValidationError(
                        "Unsupported storage type for extension geoarrow.point");
                }

                break;

            case GeoArrowMeta::StorageType::List:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, ListView<GeoArrowPointView, int32_t>, FixedWidthListView<ListView<GeoArrowPointView, int32_t>>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int32_t>, FixedWidthListView<ListView<GeoArrowPointStructView, int32_t>>>(schema);

                default:
                    throw GeoArrowMeta::ValidationError(
                        "Unsupported storage type for extension geoarrow.point");
                }

                break;

            case GeoArrowMeta::StorageType::LargeList:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, ListView<GeoArrowPointView, int64_t>, FixedWidthListView<ListView<GeoArrowPointView, int64_t>>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int64_t>, FixedWidthListView<ListView<GeoArrowPointStructView, int64_t>>>(schema);

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

    case GeoArrowMeta::StorageType::List:

            switch (linestring_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, FixedWidthListView<GeoArrowPointView>, ListView<FixedWidthListView<GeoArrowPointView>, int32_t>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, FixedWidthListView<GeoArrowPointStructView>, ListView<FixedWidthListView<GeoArrowPointStructView>, int32_t>>(schema);

                default:
                    throw GeoArrowMeta::ValidationError(
                        "Unsupported storage type for extension geoarrow.point");
                }

                break;

            case GeoArrowMeta::StorageType::List:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, ListView<GeoArrowPointView, int32_t>, ListView<ListView<GeoArrowPointView, int32_t>, int32_t>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int32_t>, ListView<ListView<GeoArrowPointStructView, int32_t>, int32_t>>(schema);

                default:
                    throw GeoArrowMeta::ValidationError(
                        "Unsupported storage type for extension geoarrow.point");
                }

                break;

            case GeoArrowMeta::StorageType::LargeList:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, ListView<GeoArrowPointView, int64_t>, ListView<ListView<GeoArrowPointView, int64_t>, int32_t>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int64_t>, ListView<ListView<GeoArrowPointStructView, int64_t>, int32_t>>(schema);

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

    case GeoArrowMeta::StorageType::LargeList:

            switch (linestring_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, FixedWidthListView<GeoArrowPointView>, ListView<FixedWidthListView<GeoArrowPointView>, int64_t>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, FixedWidthListView<GeoArrowPointStructView>, ListView<FixedWidthListView<GeoArrowPointStructView>, int64_t>>(schema);

                default:
                    throw GeoArrowMeta::ValidationError(
                        "Unsupported storage type for extension geoarrow.point");
                }

                break;

            case GeoArrowMeta::StorageType::List:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, ListView<GeoArrowPointView, int32_t>, ListView<ListView<GeoArrowPointView, int32_t>, int64_t>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int32_t>, ListView<ListView<GeoArrowPointStructView, int32_t>, int64_t>>(schema);

                default:
                    throw GeoArrowMeta::ValidationError(
                        "Unsupported storage type for extension geoarrow.point");
                }

                break;

            case GeoArrowMeta::StorageType::LargeList:

                switch (point_meta.storage_type_) {
                case GeoArrowMeta::StorageType::FixedWidthList:
                    return new GeoArrowPolygonView<GeoArrowPointView, ListView<GeoArrowPointView, int64_t>, ListView<ListView<GeoArrowPointView, int64_t>, int64_t>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowPolygonView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int64_t>, ListView<ListView<GeoArrowPointStructView, int64_t>, int64_t>>(schema);

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

GeoArrowArrayView* create_view_multipoint(struct ArrowSchema* schema,
                                          GeoArrowMeta& multi_meta, GeoArrowMeta& point_meta) {
    switch (multi_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:

            switch (point_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:
                return new GeoArrowMultiView<GeoArrowPointView, FixedWidthListView<GeoArrowPointView>>(schema);

            case GeoArrowMeta::StorageType::Struct:
                return new GeoArrowMultiView<GeoArrowPointStructView, FixedWidthListView<GeoArrowPointStructView>>(schema);

            default:
                throw GeoArrowMeta::ValidationError(
                    "Unsupported storage type for extension geoarrow.point");
            }

        break;

    case GeoArrowMeta::StorageType::List:

            switch (point_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:
                return new GeoArrowMultiView<GeoArrowPointView, ListView<GeoArrowPointView, int32_t>>(schema);

            case GeoArrowMeta::StorageType::Struct:
                return new GeoArrowMultiView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int32_t>>(schema);

            default:
                throw GeoArrowMeta::ValidationError(
                    "Unsupported storage type for extension geoarrow.point");
            }

        break;

    case GeoArrowMeta::StorageType::LargeList:

            switch (point_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:
                return new GeoArrowMultiView<GeoArrowPointView, ListView<GeoArrowPointView, int64_t>>(schema);

            case GeoArrowMeta::StorageType::Struct:
                return new GeoArrowMultiView<GeoArrowPointStructView, ListView<GeoArrowPointStructView, int64_t>>(schema);

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

GeoArrowArrayView* create_view_multilinestring(struct ArrowSchema* schema,
                                               GeoArrowMeta& multi_meta, GeoArrowMeta& linestring_meta) {
    throw GeoArrowMeta::ValidationError("Multilinestring not implemented");
}

GeoArrowArrayView* create_view_multipolygon(struct ArrowSchema* schema,
                                            GeoArrowMeta& multi_meta, GeoArrowMeta& polygon_meta) {
    GeoArrowMeta linestring_meta(schema->children[0]);
    GeoArrowMeta point_meta(schema->children[0]->children[0]);
    throw GeoArrowMeta::ValidationError("Multipolygon not implemented");
}

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
