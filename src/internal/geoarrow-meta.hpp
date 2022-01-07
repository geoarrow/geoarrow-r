
#include <cstring>
#include <stdexcept>
#include <memory>
#include <algorithm>
#include <carrow.h>


class GeoArrowMeta {
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

    class ValidationError: public std::runtime_error {
      public:
        ValidationError(const char* what): std::runtime_error(what) {}
    };

    GeoArrowMeta(const struct ArrowSchema* schema = nullptr) {
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
            snprintf(error_, 1024, "schema is NULL");
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
                snprintf(
                    error_, 1024,
                    "Expected container schema to have one child but found %lld", schema->n_children);
                return false;
            }
            break;
        default:
            break;
        }


        GeoArrowMeta child;
        int n_dims;

        switch (extension_) {
        case Extension::Point:
            n_dims = strlen(dim_);

            switch (storage_type_) {
            case StorageType::FixedWidthList:
                if (fixed_width_ != n_dims) {
                    snprintf(
                        error_, 1024,
                        "Expected geoarrow.point with dimensions '%s' to have width %d but found width %lld",
                        dim_, n_dims, fixed_width_);
                    return false;
                }
                if (!child.set_schema(schema->children[0])) {
                    snprintf(
                        error_, 1024,
                        "geoarrow.point has an invalid child schema: %s",
                        child.error_);
                    return false;
                }
                if (child.storage_type_ != Float64) {
                    snprintf(
                        error_, 1024,
                        "Expected child of fixed-width list geoarrow.point to have type Float64");
                    return false;
                }
                break;
            case StorageType::Struct:
                if (schema->n_children < n_dims) {
                    snprintf(
                        error_, 1024,
                        "Expected struct geoarrow.point with dimensions '%s' to have %d or more children but found %lld",
                        dim_, n_dims, schema->n_children);
                    return false;
                }

                for (uint64_t i = 0; i < n_dims; i++) {
                    if (!child.set_schema(schema->children[i])) {
                        snprintf(
                            error_, 1024,
                            "Struct geoarrow.point child %lld has an invalid schema: %s",
                            i, child.error_
                        );
                        return false;
                    }

                    if (child.storage_type_ != StorageType::Float64) {
                        snprintf(
                            error_, 1024,
                            "Struct geoarrow.point child %lld had an unsupported storage type '%s'",
                            i, schema->format);
                        return false;
                    }
                }

                break;
            default:
                snprintf(
                    error_, 1024,
                    "Expected geoarrow.point schema to be a struct or a fixed-width list but found '%s'",
                    schema->format);
                return false;
            }
            break;
        case Extension::Linestring:
        case Extension::Polygon:
        case Extension::Multi:
        default:
            break;
        }

        return true;
    }

    bool array_valid(const struct ArrowArray* array) {
        if (array->n_buffers != expected_buffers_) {
            snprintf(
                error_, 1024,
                "Expected array with %d buffers but found %lld",
                expected_buffers_, array->n_buffers);
            return false;
        }

        switch (storage_type_) {
        case StorageType::FixedWidthList:
        case StorageType::List:
        case StorageType::LargeList:
            if (array->n_children != 1) {
                snprintf(
                    error_, 1024,
                    "Expected container array to have one child but found %lld", array->n_children);
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

                    if (name_len >= 3 && strncmp(name, "dim", 3) == 0) {
                        memcpy(dim_, value, std::min<int32_t>(4, value_len));
                    } else if (name_len >= 3 && strncmp(name, "crs", 3) == 0) {
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

    StorageType storage_type_;
    int64_t fixed_width_;
    int32_t expected_buffers_;
    bool nullable_;

    Extension extension_;
    char dim_[5];
    bool geodesic_;
    int32_t crs_size_;
    const char* crs_;

    char error_[1024];
};
