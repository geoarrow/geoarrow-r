
#pragma once

#include <cstring>
#include <stdexcept>
#include <cstdarg>

#include "common.hpp"

namespace geoarrow {

class Meta {
  public:

    class ValidationError: public std::runtime_error {
      public:
        ValidationError(const char* what): std::runtime_error(what) {}
    };

    Meta(const struct ArrowSchema* schema = nullptr) {
        if (!set_schema(schema) && schema != nullptr) {
            throw ValidationError(error_);
        }
    }

    void reset_error() {
        memset(error_, 0, 1024);
    }

    void reset() {
        storage_type_ = util::StorageType::StorageTypeOther;
        fixed_width_ = 0;
        expected_buffers_ = -1;
        nullable_ = false;
        extension_ = util::Extension::ExtensionNone;
        edges_ = util::Edges::EdgesUnknown;
        crs_size_ = false;
        crs_ = nullptr;
        geometry_type_ = util::GeometryType::GEOMETRY_TYPE_UNKNOWN;
        dimensions_ = util::Dimensions::DIMENSIONS_UNKNOWN;
        reset_error();

        memset(dim_, 0, sizeof(dim_));
        dim_[0] = 'x';
        dim_[1] = 'y';
    }

    bool set_schema(const struct ArrowSchema* schema = nullptr) {
        reset();
        if (schema == nullptr) {
            storage_type_ = util::StorageType::StorageTypeNone;
            extension_ = util::Extension::ExtensionNone;
            set_error("schema is NULL");
            return false;
        }

        nullable_ = schema->flags & ARROW_FLAG_NULLABLE;
        walk_format(schema->format);
        walk_metadata(schema->metadata);
        return schema_valid(schema);
    }

    bool schema_valid(const struct ArrowSchema* schema) {
        switch (storage_type_) {
        case util::StorageType::FixedWidthList:
            if (fixed_width_ <= 0) {
                set_error("Expected fixed-size list to have size > 0 but got %lld", fixed_width_);
                return false;
            }
        case util::StorageType::List:
        case util::StorageType::LargeList:
            if (schema->n_children != 1) {
                set_error("Expected container schema to have one child but found %lld", schema->n_children);
                return false;
            }
            break;
        case util::StorageType::FixedWidthBinary:
            if (fixed_width_ <= 0) {
                set_error("Expected fixed-width binary to have width > 0 but got %lld", fixed_width_);
                return false;
            }
        default:
            break;
        }


        Meta child;

        switch (extension_) {
        case util::Extension::Point:
            geometry_type_ = util::GeometryType::POINT;

            switch (storage_type_) {
            case util::StorageType::FixedWidthList:
                strncpy(dim_, schema->children[0]->name, 4);
                dimensions_ = dimensions_from_dim(dim_);
                if (fixed_width_ != static_cast<int>(strlen(dim_))) {
                    set_error(
                        "Expected geoarrow.point with dimensions '%s' to have width %d but found width %lld",
                        dim_, strlen(dim_), fixed_width_);
                    return false;
                }
                if (!child.set_schema(schema->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.point has an invalid child schema");
                    return false;
                }
                if (child.storage_type_ != util::StorageType::Float64) {
                    set_error("Expected child of fixed-width list geoarrow.point to have type Float64");
                    return false;
                }
                break;
            case util::StorageType::Struct:
                for (int64_t i = 0; i < schema->n_children; i++) {
                    if (!child.set_schema(schema->children[i])) {
                        set_child_error(
                            child.error_,
                            "Struct geoarrow.point child %lld has an invalid schema", i);
                        return false;
                    }

                    if (child.storage_type_ != util::StorageType::Float64) {
                        set_error(
                            "Struct geoarrow.point child %lld had an unsupported storage type '%s'",
                            i, schema->children[i]->format);
                        return false;
                    }

                    dim_[i] = schema->children[i]->name[0];
                    if (strlen(schema->children[i]->name) != 1 || strchr("xyzm", dim_[i]) == nullptr) {
                        set_error(
                            "Expected struct geoarrow.point child %lld to have name 'x', 'y', 'z', or 'm', but found '%s'",
                            i, schema->children[i]->name);
                        return false;
                    }
                }

                dimensions_ = dimensions_from_dim(dim_);
                if (dimensions_ == util::Dimensions::DIMENSIONS_UNKNOWN) {
                    set_error(
                        "Struct geoarrow.point child names must be 'xy', 'xyz', 'xym', or 'xyzm'");
                    return false;
                }

                break;
            default:
                set_error(
                    "Expected geoarrow.point to be a struct or a fixed-width list but found '%s'",
                    schema->format);
                return false;
            }
            break;
        case util::Extension::Linestring:
            geometry_type_ = util::GeometryType::LINESTRING;
            switch (storage_type_) {
            case util::StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.linestring child has an invalid schema");
                    return false;
                }

                if (child.extension_ != util::Extension::Point) {
                    set_error("Child of geoarrow.linestring must be a geoarrow.point");
                    return false;
                }

                dimensions_ = child.dimensions_;
                crs_size_ = child.crs_size_;
                crs_ = child.crs_;
                if (edges_ == util::Edges::EdgesUnknown) {
                    edges_ = util::Edges::Planar;
                }
                break;

            default:
                set_error(
                    "Expected geoarrow.linestring to be a list but found '%s'",
                    schema->format);
                return false;
            }

            break;

        case util::Extension::Polygon:
            geometry_type_ = util::GeometryType::POLYGON;
            switch (storage_type_) {
            case util::StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.polygon child has an invalid schema");
                    return false;
                }

                if (child.storage_type_ != util::StorageType::List) {
                    set_error(
                        "Expected child of a geoarrow.polygon to be a list but found '%s'",
                        schema->children[0]->format);
                    return false;
                }

                if (!child.set_schema(schema->children[0]->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.polygon grandchild has an invalid schema");
                    return false;
                }

                if (child.extension_ != util::Extension::Point) {
                    set_error("Grandchild of geoarrow.polygon must be a geoarrow.point");
                    return false;
                }

                dimensions_ = child.dimensions_;
                crs_size_ = child.crs_size_;
                crs_ = child.crs_;
                if (edges_ == util::Edges::EdgesUnknown) {
                    edges_ = util::Edges::Planar;
                }
                break;

            default:
                set_error(
                    "Expected geoarrow.polygon to be a list but found '%s'",
                    schema->format);
                return false;
            }

            break;

        case util::Extension::MultiPoint:
        case util::Extension::MultiLinestring:
        case util::Extension::MultiPolygon:
        case util::Extension::GeometryCollection:
        switch (storage_type_) {
            case util::StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.geometrycollection child has an invalid schema");
                    return false;
                }

                switch (child.extension_) {
                case util::Extension::Point:
                    geometry_type_ = util::GeometryType::MULTIPOINT;
                    break;
                case util::Extension::Linestring:
                    geometry_type_ = util::GeometryType::MULTILINESTRING;
                    break;
                case util::Extension::Polygon:
                    geometry_type_ = util::GeometryType::MULTIPOLYGON;
                    break;
                default:
                    set_error(
                        "Child of geoarrow.geometrycollection must be a geoarrow.point, geoarrow.linestring, or geoarrow.polygon");
                    return false;
                }

                dimensions_ = child.dimensions_;
                crs_size_ = child.crs_size_;
                crs_ = child.crs_;
                edges_ = child.edges_;
                break;

            default:
                set_error(
                    "Expected geoarrow.geometrycollection to be a list but found '%s'",
                    schema->format);
                return false;
            }

            break;

        case util::Extension::WKB:
            geometry_type_ = util::GeometryType::GEOMETRY_TYPE_UNKNOWN;

            switch (storage_type_) {
            case util::StorageType::Binary:
            case util::StorageType::LargeBinary:
            case util::StorageType::FixedWidthBinary:
                break;
            default:
                set_error(
                    "Expected geoarrow.wkb to be a binary, large binary, or fixed-width binary but found '%s'",
                    schema->format);
            }
            break;

        case util::Extension::WKT:
            geometry_type_ = util::GeometryType::GEOMETRY_TYPE_UNKNOWN;

            switch (storage_type_) {
            case util::StorageType::Binary:
            case util::StorageType::LargeBinary:
            case util::StorageType::String:
            case util::StorageType::LargeString:
                break;
            default:
                set_error(
                    "Expected geoarrow.wkt to be a string or large string but found '%s'",
                    schema->format);
            }
            break;

        default:
            break;
        }

        return true;
    }

    bool array_valid(const struct ArrowArray* array) {
        if (array->n_buffers != expected_buffers_) {
            set_error(
                "Expected array with %d buffers but found %lld",
                expected_buffers_, array->n_buffers);
            return false;
        }

        switch (storage_type_) {
        case util::StorageType::FixedWidthList:
        case util::StorageType::List:
        case util::StorageType::LargeList:
            if (array->n_children != 1) {
                set_error(
                    "Expected container array to have one child but found %lld",
                    array->n_children);
                return false;
            }
            break;
        default:
            break;
        }

        return true;
    }

    void walk_format(const char* format) {
        switch (format[0]) {
        case 'f':
            storage_type_ = util::StorageType::Float32;
            expected_buffers_ = 2;
            break;
        case 'g':
            storage_type_ = util::StorageType::Float64;
            expected_buffers_ = 2;
            break;
        case 'u':
            storage_type_ = util::StorageType::String;
            expected_buffers_ = 3;
            break;
        case 'U':
            storage_type_ = util::StorageType::LargeString;
            expected_buffers_ = 3;
            break;
        case 'z':
            storage_type_ = util::StorageType::Binary;
            expected_buffers_ = 3;
            break;
        case 'Z':
            storage_type_ = util::StorageType::LargeBinary;
            expected_buffers_ = 3;
            break;
        case 'w':
            storage_type_ = util::StorageType::FixedWidthBinary;
            fixed_width_ = atol(format + 2);
            expected_buffers_ = 2;
            break;
        case '+':
            switch (format[1]) {
            case 'l':
                storage_type_ = util::StorageType::List;
                expected_buffers_ = 2;
                break;
            case 'L':
                storage_type_ = util::StorageType::LargeList;
                expected_buffers_ = 2;
                break;
            case 'w':
                storage_type_ = util::StorageType::FixedWidthList;
                fixed_width_ = atol(format + 3);
                expected_buffers_ = 1;
                break;
            case 's':
                storage_type_ = util::StorageType::Struct;
                expected_buffers_ = 1;
                break;
            }
        }
    }

    void walk_metadata(const char* metadata) {
        if (metadata == nullptr) {
            return;
        }

        int64_t pos = 0;
        int32_t n, m, name_len, value_len;
        memcpy(&n, metadata + pos, sizeof(int32_t));
        pos += sizeof(int32_t);

        for (int i = 0; i < n; i++) {
            memcpy(&name_len, metadata + pos, sizeof(int32_t));
            pos += sizeof(int32_t);

            // !! not null-terminated!
            const char* name = metadata + pos;
            pos += name_len;

            memcpy(&value_len, metadata + pos, sizeof(int32_t));
            pos += sizeof(int32_t);

            if (name_len >= 20 && strncmp(name, "ARROW:extension:name", 20) == 0) {
                // !! not null-terminated!
                const char* value = metadata + pos;
                pos += value_len;

                if (value_len >= 14 && strncmp(value, "geoarrow.point", 14) == 0) {
                    extension_ = util::Extension::Point;
                } else if (value_len >= 19 && strncmp(value, "geoarrow.linestring", 19) == 0) {
                    extension_ = util::Extension::Linestring;
                } else if (value_len >= 16 && strncmp(value, "geoarrow.polygon", 16) == 0) {
                    extension_ = util::Extension::Polygon;
                } else if (value_len >= 19 && strncmp(value, "geoarrow.multipoint", 19) == 0) {
                    extension_ = util::Extension::MultiPoint;
                } else if (value_len >= 24 && strncmp(value, "geoarrow.multilinestring", 24) == 0) {
                    extension_ = util::Extension::MultiLinestring;
                } else if (value_len >= 21 && strncmp(value, "geoarrow.multipolygon", 21) == 0) {
                    extension_ = util::Extension::MultiPolygon;
                } else if (value_len >= 14 && strncmp(value, "geoarrow.geometrycollection", 14) == 0) {
                    extension_ = util::Extension::GeometryCollection;
                } else if (value_len >= 12 && strncmp(value, "geoarrow.wkb", 12) == 0) {
                    extension_ = util::Extension::WKB;
                } else if (value_len >= 12 && strncmp(value, "geoarrow.wkt", 12) == 0) {
                    extension_ = util::Extension::WKT;
                } else {
                    extension_ = util::Extension::ExtensionOther;
                }

            } else if (name_len >= 24 && strncmp(name, "ARROW:extension:metadata", 24) == 0) {
                memcpy(&m, metadata + pos, sizeof(int32_t));
                pos += sizeof(int32_t);

                for (int j = 0; j < m; j++) {
                    memcpy(&name_len, metadata + pos, sizeof(int32_t));
                    pos += sizeof(int32_t);

                    // !! not null-terminated!
                    const char* name = metadata + pos;
                    pos += name_len;

                    memcpy(&value_len, metadata + pos, sizeof(int32_t));
                    pos += sizeof(int32_t);

                    // !! not null-terminated!
                    const char* value = metadata + pos;
                    pos += value_len;

                    if (name_len == 0 || value_len == 0) {
                        continue;
                    }

                    if (name_len >= 3 && strncmp(name, "crs", 3) == 0) {
                        crs_size_ = value_len;
                        crs_ = value;
                    } else if (name_len >= 4 && strncmp(name, "edges", 4) == 0) {
                        if (value_len >= 9 && strncmp(value, "spherical", 9) == 0) {
                            edges_ = util::Edges::Spherical;
                        } else if (value_len >= 11 && strncmp(value, "ellipsoidal", 11) == 0) {
                            edges_ = util::Edges::Ellipsoidal;
                        }
                    }
                }
            } else {
                pos += value_len;
                continue;
            }
        }
    }

    util::Dimensions dimensions_from_dim(const char* dim) {
        if (strcmp(dim, "xy") == 0) {
            return util::Dimensions::XY;
        } else if (strcmp(dim, "xyz") == 0) {
            return util::Dimensions::XYZ;
        } else if (strcmp(dim, "xym") == 0) {
            return util::Dimensions::XYM;
        } else if (strcmp(dim, "xyzm") == 0) {
            return util::Dimensions::XYZM;
        } else {
            return util::Dimensions::DIMENSIONS_UNKNOWN;
        }
    }

    util::StorageType storage_type_;
    int64_t fixed_width_;
    int32_t expected_buffers_;
    bool nullable_;

    util::Extension extension_;
    char dim_[5];
    util::Edges edges_;
    int32_t crs_size_;
    const char* crs_;

    util::GeometryType geometry_type_;
    util::Dimensions dimensions_;

    char error_[1024];

private:

    int set_error(const char* fmt, ...) {
        memset(error_, 0, sizeof(error_));
        va_list args;
        va_start(args, fmt);
        int result = vsnprintf(error_, sizeof(error_) - 1, fmt, args);
        va_end(args);
        return result;
    }

    void set_child_error(const char* child_error_, const char* fmt, ...) {
        char child_error[800];
        memset(child_error, 0, sizeof(child_error));
        memcpy(child_error, child_error_, sizeof(child_error) - 1);
        memset(error_, 0, sizeof(error_));

        va_list args;
        va_start(args, fmt);
        int result = vsnprintf(error_, sizeof(error_) - 1, fmt, args);
        va_end(args);

        snprintf(error_ + result, sizeof(error_) - result - 1, ": %s", child_error);
    }
};

}
