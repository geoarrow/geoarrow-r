
#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>

// prevent one-definition rule violations if the Arrow ABI header
// has already been loaded
#ifndef ARROW_FLAG_DICTIONARY_ORDERED
extern "C" {

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

// EXPERIMENTAL: C stream interface

struct ArrowArrayStream {
  // Callback to get the stream type
  // (will be the same for all arrays in the stream).
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowArray must be released independently from the stream.
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowArrayStream*);

  // Opaque producer-specific data
  void* private_data;
};

}
#endif


namespace geoarrow {

class Meta {
  public:

    enum Extension {
        Point,
        Linestring,
        Polygon,
        Multi,
        Unknown,
        ExtensionNone
    };

    enum StorageType {
        Float32,
        Float64,
        String,
        FixedWidthBinary,
        Binary,
        LargeBinary,
        FixedWidthList,
        Struct,
        List,
        LargeList,
        Other,
        StorageTypeNone
    };

    enum GeometryType {
        GEOMETRY_TYPE_UNKNOWN = 0,
        POINT = 1,
        LINESTRING = 2,
        POLYGON = 3,
        MULTIPOINT = 4,
        MULTILINESTRING = 5,
        MULTIPOLYGON = 6,
        GEOMETRYCOLLECTION = 7
    };

    enum Dimensions {DIMENSIONS_UNKNOWN = 0, XY = 1, XYZ = 2, XYM = 3, XYZM = 4};

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
        storage_type_ = StorageType::Other;
        fixed_width_ = 0;
        expected_buffers_ = -1;
        nullable_ = false;
        extension_ = Extension::Unknown;
        geodesic_ = false;
        crs_size_ = false;
        crs_ = nullptr;
        geometry_type_ = GeometryType::GEOMETRY_TYPE_UNKNOWN;
        dimensions_ = Dimensions::DIMENSIONS_UNKNOWN;
        reset_error();

        memset(dim_, 0, sizeof(dim_));
        dim_[0] = 'x';
        dim_[1] = 'y';
    }

    bool set_schema(const struct ArrowSchema* schema = nullptr) {
        reset();
        if (schema == nullptr) {
            storage_type_ = StorageType::StorageTypeNone;
            extension_ = Extension::ExtensionNone;
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
        case StorageType::FixedWidthList:
        case StorageType::List:
        case StorageType::LargeList:
            if (schema->n_children != 1) {
                set_error("Expected container schema to have one child but found %lld", schema->n_children);
                return false;
            }
            break;
        default:
            break;
        }


        Meta child;

        switch (extension_) {
        case Extension::Point:
            geometry_type_ = GeometryType::POINT;

            switch (storage_type_) {
            case StorageType::FixedWidthList:
                strncpy(dim_, schema->children[0]->name, 4);
                dimensions_ = dimensions_from_dim(dim_);
                if (fixed_width_ != strlen(dim_)) {
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
                if (child.storage_type_ != Float64) {
                    set_error("Expected child of fixed-width list geoarrow.point to have type Float64");
                    return false;
                }
                break;
            case StorageType::Struct:
                for (uint64_t i = 0; i < schema->n_children; i++) {
                    if (!child.set_schema(schema->children[i])) {
                        set_child_error(
                            child.error_,
                            "Struct geoarrow.point child %lld has an invalid schema", i);
                        return false;
                    }

                    if (child.storage_type_ != StorageType::Float64) {
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
                if (dimensions_ == Dimensions::DIMENSIONS_UNKNOWN) {
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
        case Extension::Linestring:
            geometry_type_ = GeometryType::LINESTRING;
            switch (storage_type_) {
            case StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.linestring child has an invalid schema");
                    return false;
                }

                if (child.extension_ != Extension::Point) {
                    set_error("Child of geoarrow.linestring must be a geoarrow.point");
                    return false;
                }

                dimensions_ = child.dimensions_;
                crs_size_ = child.crs_size_;
                crs_ = child.crs_;
                break;

            default:
                set_error(
                    "Expected geoarrow.linestring to be a list but found '%s'",
                    schema->format);
                return false;
            }

            break;

        case Extension::Polygon:
            geometry_type_ = GeometryType::POLYGON;
            switch (storage_type_) {
            case StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.polygon child has an invalid schema");
                    return false;
                }

                if (child.storage_type_ != StorageType::List) {
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

                if (child.extension_ != Extension::Point) {
                    set_error("Grandchild of geoarrow.polygon must be a geoarrow.point");
                    return false;
                }

                dimensions_ = child.dimensions_;
                crs_size_ = child.crs_size_;
                crs_ = child.crs_;
                break;

            default:
                set_error(
                    "Expected geoarrow.polygon to be a list but found '%s'",
                    schema->format);
                return false;
            }

            break;

        case Extension::Multi:
        switch (storage_type_) {
            case StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    set_child_error(
                        child.error_,
                        "geoarrow.multi child has an invalid schema");
                    return false;
                }

                switch (child.extension_) {
                case Extension::Point:
                    geometry_type_ = GeometryType::MULTIPOINT;
                    break;
                case Extension::Linestring:
                    geometry_type_ = GeometryType::MULTILINESTRING;
                    break;
                case Extension::Polygon:
                    geometry_type_ = GeometryType::MULTIPOLYGON;
                    break;
                default:
                    set_error(
                        "Child of geoarrow.multi must be a geoarrow.point, geoarrow.linestring, or geoarrow.polygon");
                    return false;
                }

                dimensions_ = child.dimensions_;
                crs_size_ = child.crs_size_;
                crs_ = child.crs_;
                geodesic_ = child.geodesic_;
                break;

            default:
                set_error(
                    "Expected geoarrow.multi to be a list but found '%s'",
                    schema->format);
                return false;
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
        case StorageType::FixedWidthList:
        case StorageType::List:
        case StorageType::LargeList:
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
            storage_type_ = StorageType::Float32;
            expected_buffers_ = 2;
            break;
        case 'g':
            storage_type_ = StorageType::Float64;
            expected_buffers_ = 2;
            break;
        case 'u':
            storage_type_ = StorageType::String;
            expected_buffers_ = 3;
            break;
        case 'z':
            storage_type_ = StorageType::Binary;
            expected_buffers_ = 3;
            break;
        case 'Z':
            storage_type_ = StorageType::LargeBinary;
            expected_buffers_ = 3;
            break;
        case 'w':
            storage_type_ = StorageType::FixedWidthBinary;
            fixed_width_ = atol(format + 2);
            expected_buffers_ = 2;
            break;
        case '+':
            switch (format[1]) {
            case 'l':
                storage_type_ = StorageType::List;
                expected_buffers_ = 2;
                break;
            case 'L':
                storage_type_ = StorageType::LargeList;
                expected_buffers_ = 2;
                break;
            case 'w':
                storage_type_ = StorageType::FixedWidthList;
                fixed_width_ = atol(format + 3);
                expected_buffers_ = 1;
                break;
            case 's':
                storage_type_ = StorageType::Struct;
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
                    extension_ = Extension::Point;
                } else if (value_len >= 19 && strncmp(value, "geoarrow.linestring", 19) == 0) {
                    extension_ = Extension::Linestring;
                } else if (value_len >= 16 && strncmp(value, "geoarrow.polygon", 16) == 0) {
                    extension_ = Extension::Polygon;
                } else if (value_len >= 14 && strncmp(value, "geoarrow.multi", 14) == 0) {
                    extension_ = Extension::Multi;
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
                    } else if (name_len >= 3 && strncmp(name, "geodesic", 3) == 0) {
                        if (value_len >= 4 && strncmp(value, "true", 4) == 0) {
                            geodesic_ = true;
                        }
                    }
                }
            } else {
                pos += value_len;
                continue;
            }
        }
    }

    Dimensions dimensions_from_dim(const char* dim) {
        if (strcmp(dim, "xy") == 0) {
            return Dimensions::XY;
        } else if (strcmp(dim, "xyz") == 0) {
            return Dimensions::XYZ;
        } else if (strcmp(dim, "xym") == 0) {
            return Dimensions::XYM;
        } else if (strcmp(dim, "xyzm") == 0) {
            return Dimensions::XYZM;
        } else {
            return Dimensions::DIMENSIONS_UNKNOWN;
        }
    }

    StorageType storage_type_;
    int64_t fixed_width_;
    int32_t expected_buffers_;
    bool nullable_;

    Extension extension_;
    char dim_[5];
    bool geodesic_;
    int32_t crs_size_;
    const char* crs_;

    GeometryType geometry_type_;
    Dimensions dimensions_;

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
