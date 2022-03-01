// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
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


        GeoArrowMeta child;

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

// A `GeoArrowHandler` is a stateful handler base class that responds to events
// as they are encountered while iterating over a `GeoArrowArrayView`. This
// style of iteration is useful for certain types of operations, particularly
// if virtual method calls are a concern. You can also use `GeoArrowArrayView`'s
// pull-style iterators to iterate over geometries.
class GeoArrowHandler {
public:
    enum Result {
        CONTINUE = 0,
        ABORT = 1,
        ABORT_FEATURE = 2
    };

    virtual void schema(const struct ArrowSchema* schema) {}
    virtual void new_geometry_type(GeoArrowMeta::GeometryType geometry_type) {}
    virtual void new_dimensions(GeoArrowMeta::Dimensions geometry_type) {}

    virtual Result array_start(const struct ArrowArray* array_data) { return Result::CONTINUE; }
    virtual Result feat_start() { return Result::CONTINUE; }
    virtual Result null_feat() { return Result::CONTINUE; }
    virtual Result geom_start(int32_t size) { return Result::CONTINUE; }
    virtual Result ring_start(int32_t size) { return Result::CONTINUE; }
    virtual Result coord(const double* coord) { return Result::CONTINUE; }
    virtual Result ring_end() { return Result::CONTINUE; }
    virtual Result geom_end() { return Result::CONTINUE; }
    virtual Result feat_end() { return Result::CONTINUE; }
    virtual Result array_end() { return Result::CONTINUE; }

    virtual ~GeoArrowHandler() {}
};


#define HANDLE_OR_RETURN(expr)                                 \
    result = expr;                                             \
    if (result != GeoArrowHandler::Result::CONTINUE) return result


#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == GeoArrowHandler::Result::ABORT_FEATURE) \
        continue; \
    else if (result == GeoArrowHandler::Result::ABORT) break


namespace {

    template <class ArrayView>
    GeoArrowHandler::Result read_point_geometry(ArrayView& view, GeoArrowHandler* handler, int64_t offset) {
        GeoArrowHandler::Result result;
        HANDLE_OR_RETURN(handler->geom_start(1));
        HANDLE_OR_RETURN(view.read_coord(handler, offset));
        HANDLE_OR_RETURN(handler->geom_end());
        return GeoArrowHandler::Result::CONTINUE;
    }

    template <class ArrayView>
    GeoArrowHandler::Result read_feature_templ(ArrayView& view, int64_t offset, GeoArrowHandler* handler) {
        GeoArrowHandler::Result result;
        HANDLE_OR_RETURN(handler->feat_start());

        if (view.is_null(offset)) {
            HANDLE_OR_RETURN(handler->null_feat());
        } else {
            HANDLE_OR_RETURN(view.read_geometry(handler, offset));
        }

        HANDLE_OR_RETURN(handler->feat_end());
        return GeoArrowHandler::Result::CONTINUE;
    }

    template <class ArrayView>
    GeoArrowHandler::Result read_features_templ(ArrayView& view, GeoArrowHandler* handler) {
        GeoArrowHandler::Result result;

        for (uint64_t i = 0; i < view.array_->length; i++) {
            HANDLE_CONTINUE_OR_BREAK(view.read_feature(handler, i));
        }

        if (result == GeoArrowHandler::Result::ABORT) {
            return GeoArrowHandler::Result::ABORT;
        } else {
            return GeoArrowHandler::Result::CONTINUE;
        }
    }

} // anonymous namespace


// The `GeoArrowArrayView` is the main class that uses of the API will interact with.
// it is an abstract class that represents a view of a `struct ArrowSchema` and
// a sequence of `struct ArrowArray`s, both of which must be valid pointers for the
// lifetime of the `GeoArrowArrayView`. The `GeoArrowArrayView` supports handler-style
// iteration using a `GeoArrowHandler` and pull-style iteration using virtual methods.
// The handler-style iteration is particularly useful as a way to write general-purpose
// code without using virtual methods.
class GeoArrowArrayView {
  public:
    GeoArrowArrayView(const struct ArrowSchema* schema):
      schema_(schema), array_(nullptr), meta_(schema),
      feature_id_(-1), validity_buffer_(nullptr) {}

    virtual ~GeoArrowArrayView() {}

    void read_meta(GeoArrowHandler* handler) {
        handler->schema(schema_);
        handler->new_geometry_type(meta_.geometry_type_);
        handler->new_dimensions(meta_.dimensions_);
    }

    virtual GeoArrowHandler::Result read_features(GeoArrowHandler* handler) {
        throw std::runtime_error("GeoArrowArrayView::read_features() not implemented");
    }

    virtual void set_array(const struct ArrowArray* array) {
        if (!meta_.array_valid(array)) {
            throw GeoArrowMeta::ValidationError(meta_.error_);
        }

        array_ = array;
        validity_buffer_ = reinterpret_cast<const uint8_t*>(array->buffers[0]);
    }

    bool is_null(int64_t offset) {
        return validity_buffer_ &&
            (validity_buffer_[offset / 8] & (0x01 << (offset % 8))) == 0;
    }

    const struct ArrowSchema* schema_;
    const struct ArrowArray* array_;
    GeoArrowMeta meta_;
    int64_t feature_id_;
    const uint8_t* validity_buffer_;
};


class GeoArrowPointView: public GeoArrowArrayView {
  public:
    GeoArrowPointView(const struct ArrowSchema* schema):
      GeoArrowArrayView(schema), data_buffer_(nullptr) {
        coord_size_ = meta_.fixed_width_;
    }

    void set_array(const struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        data_buffer_ = reinterpret_cast<const double*>(array->children[0]->buffers[1]);
    }

    GeoArrowHandler::Result read_features(GeoArrowHandler* handler) {
        return read_features_templ<GeoArrowPointView>(*this, handler);
    }

    GeoArrowHandler::Result read_feature(GeoArrowHandler* handler, int64_t offset) {
        return read_feature_templ<GeoArrowPointView>(*this, offset, handler);
    }

    GeoArrowHandler::Result read_geometry(GeoArrowHandler* handler, int64_t offset) {
        return read_point_geometry<GeoArrowPointView>(*this, handler, offset);
    }

    GeoArrowHandler::Result read_coord(GeoArrowHandler* handler, int64_t offset) {
        GeoArrowHandler::Result result;
        HANDLE_OR_RETURN(handler->coord(data_buffer_ + (offset + array_->offset) * coord_size_));
        return GeoArrowHandler::Result::CONTINUE;
    }

    int coord_size_;
    const double* data_buffer_;
};


class GeoArrowPointStructView: public GeoArrowArrayView {
  public:
    GeoArrowPointStructView(const struct ArrowSchema* schema): GeoArrowArrayView(schema) {
        switch (meta_.dimensions_) {
        case GeoArrowMeta::Dimensions::XYZ:
        case GeoArrowMeta::Dimensions::XYM:
            coord_size_ = 3;
            break;
        case GeoArrowMeta::Dimensions::XYZM:
            coord_size_ = 4;
            break;
        default:
            coord_size_ = 2;
            break;
        }

        memset(coord_buffer_, 0, sizeof(coord_buffer_));
    }

    void set_array(const struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        memset(coord_buffer_, 0, sizeof(coord_buffer_));
        for (int i = 0; i < coord_size_; i++) {
            const void* buffer_void = array->children[i]->buffers[1];
            coord_buffer_[i] = reinterpret_cast<const double*>(buffer_void);
        }
    }

    GeoArrowHandler::Result read_features(GeoArrowHandler* handler) {
        return read_features_templ<GeoArrowPointStructView>(*this, handler);
    }

    GeoArrowHandler::Result read_feature(GeoArrowHandler* handler, int64_t offset) {
        return read_feature_templ<GeoArrowPointStructView>(*this, offset, handler);
    }

    GeoArrowHandler::Result read_geometry(GeoArrowHandler* handler, int64_t offset) {
        return read_point_geometry<GeoArrowPointStructView>(*this, handler, offset);
    }

    GeoArrowHandler::Result read_coord(GeoArrowHandler* handler, int64_t offset) {
        GeoArrowHandler::Result result;

        for (int i = 0; i < coord_size_; i++) {
            coord_[i] = coord_buffer_[i][array_->offset + offset];
        }

        HANDLE_OR_RETURN(handler->coord(coord_));
        return GeoArrowHandler::Result::CONTINUE;
    }

    int coord_size_;
    double coord_[4];
    const double* coord_buffer_[4];
};


template <class ChildView>
class ListView: public GeoArrowArrayView {
  public:
    ListView(const struct ArrowSchema* schema):
      GeoArrowArrayView(schema), child_(schema->children[0]), offset_buffer_(nullptr) {}

    void set_array(const struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        child_.set_array(array->children[0]);
        offset_buffer_ = reinterpret_cast<const int32_t*>(array->buffers[1]);
    }

    int64_t child_offset(int32_t offset) {
        return offset_buffer_[array_->offset + offset];
    }

    int64_t child_size(int32_t offset) {
        return child_offset(offset + 1) - child_offset(offset);
    }

    ChildView child_;
    const int32_t* offset_buffer_;
};


template <class PointView = GeoArrowPointView>
class GeoArrowLinestringView: public ListView<PointView> {
  public:
    GeoArrowLinestringView(struct ArrowSchema* schema): ListView<PointView>(schema) {}

    GeoArrowHandler::Result read_features(GeoArrowHandler* handler) {
        return read_features_templ<GeoArrowLinestringView>(*this, handler);
    }

    GeoArrowHandler::Result read_feature(GeoArrowHandler* handler, int64_t offset) {
        return read_feature_templ<GeoArrowLinestringView>(*this, offset, handler);
    }

    GeoArrowHandler::Result read_geometry(GeoArrowHandler* handler, int64_t offset) {
        GeoArrowHandler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(size));
        for (int64_t i = 0; i < size; i++) {
            HANDLE_OR_RETURN(this->child_.read_coord(handler, initial_child_offset + i));
        }

        HANDLE_OR_RETURN(handler->geom_end());
        return GeoArrowHandler::Result::CONTINUE;
    }
};


template <class PointView = GeoArrowPointView>
class GeoArrowPolygonView: public ListView<ListView<PointView>> {
  public:
    GeoArrowPolygonView(struct ArrowSchema* schema): ListView<ListView<PointView>>(schema) {}

    GeoArrowHandler::Result read_features(GeoArrowHandler* handler) {
        return read_features_templ<GeoArrowPolygonView>(*this, handler);
    }

    GeoArrowHandler::Result read_feature(GeoArrowHandler* handler, int64_t offset) {
        return read_feature_templ<GeoArrowPolygonView>(*this, offset, handler);
    }

    GeoArrowHandler::Result read_geometry(GeoArrowHandler* handler, int64_t offset) {
        GeoArrowHandler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(size));
        for (int64_t i = 0; i < size; i++) {

            int64_t initial_coord_offset = this->child_.child_offset(initial_child_offset + i);
            int64_t ring_size = this->child_.child_size(initial_child_offset + i);

            HANDLE_OR_RETURN(handler->ring_start(ring_size));
            for (int64_t j = 0; j < ring_size; j++) {
                HANDLE_OR_RETURN(this->child_.child_.read_coord(handler, initial_coord_offset + j));
            }
            HANDLE_OR_RETURN(handler->ring_end());
        }

        HANDLE_OR_RETURN(handler->geom_end());
        return GeoArrowHandler::Result::CONTINUE;
    }

};


template <class ChildView>
class GeoArrowMultiView: public ListView<ChildView> {
  public:
    GeoArrowMultiView(struct ArrowSchema* schema): ListView<ChildView>(schema) {}

    GeoArrowHandler::Result read_features(GeoArrowHandler* handler) {
        return read_features_templ<GeoArrowMultiView>(*this, handler);
    }

    GeoArrowHandler::Result read_feature(GeoArrowHandler* handler, int64_t offset) {
        return read_feature_templ<GeoArrowMultiView>(*this, offset, handler);
    }

    GeoArrowHandler::Result read_geometry(GeoArrowHandler* handler, int64_t offset) {
        GeoArrowHandler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(size));
        for (int64_t i = 0; i < size; i++) {
            HANDLE_OR_RETURN(this->child_.read_geometry(handler, initial_child_offset + i));
        }
        HANDLE_OR_RETURN(handler->geom_end());
        return GeoArrowHandler::Result::CONTINUE;
    }
};

namespace {

// autogen factory start
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

GeoArrowArrayView* create_view_linestring(struct ArrowSchema* schema,
                                          GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]);


    switch (linestring_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:

            switch (point_meta.storage_type_) {
            case GeoArrowMeta::StorageType::FixedWidthList:
                return new GeoArrowLinestringView<GeoArrowPointView>(schema);

            case GeoArrowMeta::StorageType::Struct:
                return new GeoArrowLinestringView<GeoArrowPointStructView>(schema);

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
    case GeoArrowMeta::StorageType::List:

            switch (linestring_meta.storage_type_) {
            case GeoArrowMeta::StorageType::List:

                        switch (point_meta.storage_type_) {
                        case GeoArrowMeta::StorageType::FixedWidthList:
                            return new GeoArrowPolygonView<GeoArrowPointView>(schema);

                        case GeoArrowMeta::StorageType::Struct:
                            return new GeoArrowPolygonView<GeoArrowPointStructView>(schema);

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
    case GeoArrowMeta::StorageType::List:
        switch (point_meta.storage_type_) {
    case GeoArrowMeta::StorageType::FixedWidthList:
        return new GeoArrowMultiView<GeoArrowPointView>(schema);

    case GeoArrowMeta::StorageType::Struct:
        return new GeoArrowMultiView<GeoArrowPointStructView>(schema);

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
                                               GeoArrowMeta& multi_meta,
                                               GeoArrowMeta& linestring_meta) {
    GeoArrowMeta point_meta(schema->children[0]->children[0]);


    switch (multi_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:
        switch (linestring_meta.storage_type_) {
    case GeoArrowMeta::StorageType::List:

        switch (point_meta.storage_type_) {
        case GeoArrowMeta::StorageType::FixedWidthList:
            return new GeoArrowMultiView<GeoArrowLinestringView<GeoArrowPointView>>(schema);

        case GeoArrowMeta::StorageType::Struct:
            return new GeoArrowMultiView<GeoArrowLinestringView<GeoArrowPointStructView>>(schema);

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

GeoArrowArrayView* create_view_multipolygon(struct ArrowSchema* schema,
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
                    return new GeoArrowMultiView<GeoArrowPolygonView<GeoArrowPointView>>(schema);

                case GeoArrowMeta::StorageType::Struct:
                    return new GeoArrowMultiView<GeoArrowPolygonView<GeoArrowPointStructView>>(schema);

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

} // anonymous namespace

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
