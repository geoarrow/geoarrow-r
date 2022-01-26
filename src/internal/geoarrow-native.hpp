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


#include "wk-v1.h"

#define HANDLE_OR_RETURN(expr)                                 \
    result = expr;                                             \
    if (result != WK_CONTINUE) return result


#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == WK_ABORT_FEATURE) continue; else if (result == WK_ABORT) break


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
                    // straight using child.error_ gives a format warning because the
                    // complier knows that it could contain some characters and might
                    // truncate (which is fine, but we need the warning to go away).
                    char child_error[800];
                    memset(child_error, 0, sizeof(child_error));
                    memcpy(child_error, child.error_, sizeof(child_error) - 1);
                    snprintf(
                        error_, 1024,
                        "geoarrow.point has an invalid child schema: %s",
                        child_error);
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
                        // see above note about snprintf(..., child.error_)
                        char child_error[800];
                        memset(child_error, 0, sizeof(child_error));
                        memcpy(child_error, child.error_, sizeof(child_error) - 1);
                        snprintf(
                            error_, 1024,
                            "Struct geoarrow.point child %lld has an invalid schema: %s",
                            i, child_error);
                        return false;
                    }

                    if (child.storage_type_ != StorageType::Float64) {
                        snprintf(
                            error_, 1024,
                            "Struct geoarrow.point child %lld had an unsupported storage type '%s'",
                            i, schema->children[i]->format);
                        return false;
                    }
                }

                break;
            default:
                snprintf(
                    error_, 1024,
                    "Expected geoarrow.point to be a struct or a fixed-width list but found '%s'",
                    schema->format);
                return false;
            }
            break;
        case Extension::Linestring:
            switch (storage_type_) {
            case StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    // see above note about snprintf(..., child.error_)
                    char child_error[800];
                    memset(child_error, 0, sizeof(child_error));
                    memcpy(child_error, child.error_, sizeof(child_error) - 1);
                    snprintf(
                        error_, 1024,
                        "geoarrow.linestring child has an invalid schema: %s",
                        child_error);
                    return false;
                }

                if (child.extension_ != Extension::Point) {
                    snprintf(
                        error_, 1024,
                        "Child of geoarrow.linestring must be a geoarrow.point");
                    return false;
                }

                break;

            default:
                snprintf(
                    error_, 1024,
                    "Expected geoarrow.linestring to be a list but found '%s'",
                    schema->format);
                return false;
            }

            break;

        case Extension::Polygon:
            switch (storage_type_) {
            case StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    // see above note about snprintf(..., child.error_)
                    char child_error[800];
                    memset(child_error, 0, sizeof(child_error));
                    memcpy(child_error, child.error_, sizeof(child_error) - 1);
                    snprintf(
                        error_, 1024,
                        "geoarrow.polygon child has an invalid schema: %s",
                        child_error);
                    return false;
                }

                if (child.storage_type_ != StorageType::List) {
                    snprintf(
                        error_, 1024,
                        "Expected child of a geoarrow.polygon to be a list but found '%s'",
                        schema->children[0]->format);
                    return false;
                }

                if (!child.set_schema(schema->children[0]->children[0])) {
                    // see above note about snprintf(..., child.error_)
                    char child_error[800];
                    memset(child_error, 0, sizeof(child_error));
                    memcpy(child_error, child.error_, sizeof(child_error) - 1);
                    snprintf(
                        error_, 1024,
                        "geoarrow.polygon grandchild has an invalid schema: %s",
                        child_error);
                    return false;
                }

                if (child.extension_ != Extension::Point) {
                    snprintf(
                        error_, 1024,
                        "Grandchild of geoarrow.polygon must be a geoarrow.point");
                    return false;
                }

                break;

            default:
                snprintf(
                    error_, 1024,
                    "Expected geoarrow.polygon to be a list but found '%s'",
                    schema->format);
                return false;
            }

            break;

        case Extension::Multi:
        switch (storage_type_) {
            case StorageType::List:
                if (!child.set_schema(schema->children[0])) {
                    // see above note about snprintf(..., child.error_)
                    char child_error[800];
                    memset(child_error, 0, sizeof(child_error));
                    memcpy(child_error, child.error_, sizeof(child_error) - 1);
                    snprintf(
                        error_, 1024,
                        "geoarrow.multi child has an invalid schema: %s",
                        child_error);
                    return false;
                }

                if (child.extension_ != Extension::Point &&
                    child.extension_ != Extension::Linestring &&
                    child.extension_ != Extension::Polygon) {
                    snprintf(
                        error_, 1024,
                        "Child of geoarrow.multi must be a geoarrow.point, geoarrow.linestring, or geoarrow.polygon");
                    return false;
                }

                break;

            default:
                snprintf(
                    error_, 1024,
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


namespace {

    template <class ArrayView>
    int read_point_geometry(ArrayView& view, wk_handler_t* handler, int64_t offset, int64_t part_id = WK_PART_ID_NONE) {
        int result;
        HANDLE_OR_RETURN(handler->geometry_start(&view.meta_, part_id, handler->handler_data));
        HANDLE_OR_RETURN(view.read_coord(handler, offset, 0));
        HANDLE_OR_RETURN(handler->geometry_end(&view.meta_, part_id, handler->handler_data));
        return WK_CONTINUE;
    }


    template <class ArrayView>
    int read_feature_templ(ArrayView& view, int64_t offset, wk_handler_t* handler) {
        int result;
        view.feature_id_++;
        HANDLE_OR_RETURN(handler->feature_start(&view.vector_meta_, view.feature_id_, handler->handler_data));

        if (view.is_null(offset)) {
            HANDLE_OR_RETURN(handler->null_feature(handler->handler_data));
        } else {
            HANDLE_OR_RETURN(view.read_geometry(handler, offset, WK_PART_ID_NONE));
        }

        HANDLE_OR_RETURN(handler->feature_end(&view.vector_meta_, view.feature_id_, handler->handler_data));
        return WK_CONTINUE;
    }


    template <class ArrayView>
    int read_features_templ(ArrayView& view, wk_handler_t* handler) {
        int result;

        for (uint64_t i = 0; i < view.array_->length; i++) {
            HANDLE_CONTINUE_OR_BREAK(view.read_feature(handler, i));
        }

        if (result == WK_ABORT) {
            return WK_ABORT;
        } else {
            return WK_CONTINUE;
        }
    }

} // anonymous namespace

class GeoArrowArrayView {
  public:
    GeoArrowArrayView(const struct ArrowSchema* schema):
      schema_(schema), array_(nullptr), geoarrow_meta_(schema),
      feature_id_(-1), validity_buffer_(nullptr) {
        WK_META_RESET(meta_, WK_GEOMETRY);
        WK_VECTOR_META_RESET(vector_meta_, WK_GEOMETRY);

        if (strcmp(geoarrow_meta_.dim_, "xyz") == 0 || strcmp(geoarrow_meta_.dim_, "xyzm") == 0) {
            meta_.flags |= WK_FLAG_HAS_Z;
            vector_meta_.flags |= WK_FLAG_HAS_Z;
        }

        if (strcmp(geoarrow_meta_.dim_, "xym") == 0 || strcmp(geoarrow_meta_.dim_, "xyzm") == 0) {
            meta_.flags |= WK_FLAG_HAS_M;
            vector_meta_.flags |= WK_FLAG_HAS_M;
        }
    }

    virtual ~GeoArrowArrayView() {}

    virtual int read_features(wk_handler_t* handler) {
        throw std::runtime_error("GeoArrowArrayView::read_features() not implemented");
    }

    virtual void set_array(const struct ArrowArray* array) {
        if (!geoarrow_meta_.array_valid(array)) {
            throw GeoArrowMeta::ValidationError(geoarrow_meta_.error_);
        }

        array_ = array;
        validity_buffer_ = reinterpret_cast<const uint8_t*>(array->buffers[0]);
    }

    void set_vector_size(int64_t size) {
        vector_meta_.size = size;
    }

    bool is_null(int64_t offset) {
        return validity_buffer_ &&
            (validity_buffer_[offset / 8] & (0x01 << (offset % 8))) == 0;
    }

    wk_meta_t meta_;
    wk_vector_meta_t vector_meta_;
    const struct ArrowSchema* schema_;
    const struct ArrowArray* array_;
    GeoArrowMeta geoarrow_meta_;
    int64_t feature_id_;
    const uint8_t* validity_buffer_;
};


class GeoArrowPointView: public GeoArrowArrayView {
  public:
    GeoArrowPointView(const struct ArrowSchema* schema):
      GeoArrowArrayView(schema), data_buffer_(nullptr) {
        meta_.geometry_type = WK_POINT;
        vector_meta_.geometry_type = WK_POINT;
        meta_.size = 1;
        coord_size_ = geoarrow_meta_.fixed_width_;
    }

    void set_array(const struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        data_buffer_ = reinterpret_cast<const double*>(array->children[0]->buffers[1]);
    }

    int read_features(wk_handler_t* handler) {
        return read_features_templ<GeoArrowPointView>(*this, handler);
    }

    int read_feature(wk_handler_t* handler, int64_t offset) {
        return read_feature_templ<GeoArrowPointView>(*this, offset, handler);
    }

    int read_geometry(wk_handler_t* handler, int64_t offset, uint32_t part_id = WK_PART_ID_NONE) {
        return read_point_geometry<GeoArrowPointView>(*this, handler, offset, part_id);
    }

    int read_coord(wk_handler_t* handler, int64_t offset, int64_t coord_id = 0) {
        int result;
        HANDLE_OR_RETURN(handler->coord(
            &meta_,
            data_buffer_ + (offset + array_->offset) * coord_size_,
            coord_id,
            handler->handler_data));
        return WK_CONTINUE;
    }

    int coord_size_;
    const double* data_buffer_;
};


class GeoArrowPointStructView: public GeoArrowArrayView {
  public:
    GeoArrowPointStructView(const struct ArrowSchema* schema): GeoArrowArrayView(schema) {
        meta_.geometry_type = WK_POINT;
        vector_meta_.geometry_type = WK_POINT;
        meta_.size = 1;

        coord_size_ = 2;
        if (vector_meta_.flags & WK_FLAG_HAS_Z) coord_size_++;
        if (vector_meta_.flags & WK_FLAG_HAS_M) coord_size_++;
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

    int read_features(wk_handler_t* handler) {
        return read_features_templ<GeoArrowPointStructView>(*this, handler);
    }

    int read_feature(wk_handler_t* handler, int64_t offset) {
        return read_feature_templ<GeoArrowPointStructView>(*this, offset, handler);
    }

    int read_geometry(wk_handler_t* handler, int64_t offset, uint32_t part_id = WK_PART_ID_NONE) {
        return read_point_geometry<GeoArrowPointStructView>(*this, handler, offset, part_id);
    }

    int read_coord(wk_handler_t* handler, int64_t offset, int64_t coord_id = 0) {
        int result;

        for (int i = 0; i < coord_size_; i++) {
            coord_[i] = coord_buffer_[i][array_->offset + offset];
        }

        HANDLE_OR_RETURN(handler->coord(
            &meta_,
            coord_,
            coord_id,
            handler->handler_data));

        return WK_CONTINUE;
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
    GeoArrowLinestringView(struct ArrowSchema* schema): ListView<PointView>(schema) {
        this->meta_.geometry_type = WK_LINESTRING;
        this->vector_meta_.geometry_type = WK_LINESTRING;

        if (this->child_.meta_.flags & WK_FLAG_HAS_Z) {
            this->meta_.flags |= WK_FLAG_HAS_Z;
            this->vector_meta_.flags |= WK_FLAG_HAS_Z;
        }

        if (this->child_.meta_.flags & WK_FLAG_HAS_M) {
            this->meta_.flags |= WK_FLAG_HAS_M;
            this->vector_meta_.flags |= WK_FLAG_HAS_M;
        }
    }

    int read_features(wk_handler_t* handler) {
        return read_features_templ<GeoArrowLinestringView>(*this, handler);
    }

    int read_feature(wk_handler_t* handler, int64_t offset) {
        return read_feature_templ<GeoArrowLinestringView>(*this, offset, handler);
    }

    int read_geometry(wk_handler_t* handler, int64_t offset, uint32_t part_id = WK_PART_ID_NONE) {
        int result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);
        this->meta_.size = size;

        HANDLE_OR_RETURN(handler->geometry_start(&this->meta_, part_id, handler->handler_data));
        for (int64_t i = 0; i < size; i++) {
            HANDLE_OR_RETURN(this->child_.read_coord(handler, initial_child_offset + i, i));
        }

        HANDLE_OR_RETURN(handler->geometry_end(&this->meta_, part_id, handler->handler_data));
        return WK_CONTINUE;
    }
};


template <class PointView = GeoArrowPointView>
class GeoArrowPolygonView: public ListView<ListView<PointView>> {
  public:
    GeoArrowPolygonView(struct ArrowSchema* schema): ListView<ListView<PointView>>(schema) {
        this->meta_.geometry_type = WK_POLYGON;
        this->vector_meta_.geometry_type = WK_POLYGON;

        if (this->child_.child_.meta_.flags & WK_FLAG_HAS_Z) {
            this->meta_.flags |= WK_FLAG_HAS_Z;
            this->vector_meta_.flags |= WK_FLAG_HAS_Z;
        }

        if (this->child_.child_.meta_.flags & WK_FLAG_HAS_M) {
            this->meta_.flags |= WK_FLAG_HAS_M;
            this->vector_meta_.flags |= WK_FLAG_HAS_M;
        }
    }

    int read_features(wk_handler_t* handler) {
        return read_features_templ<GeoArrowPolygonView>(*this, handler);
    }

    int read_feature(wk_handler_t* handler, int64_t offset) {
        return read_feature_templ<GeoArrowPolygonView>(*this, offset, handler);
    }

    int read_geometry(wk_handler_t* handler, int64_t offset, uint32_t part_id = WK_PART_ID_NONE) {
        int result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);
        this->meta_.size = size;

        HANDLE_OR_RETURN(handler->geometry_start(&this->meta_, part_id, handler->handler_data));
        for (int64_t i = 0; i < size; i++) {

            int64_t initial_coord_offset = this->child_.child_offset(initial_child_offset + i);
            int64_t ring_size = this->child_.child_size(initial_child_offset + i);

            HANDLE_OR_RETURN(handler->ring_start(&this->meta_, ring_size, i, handler->handler_data));
            for (int64_t j = 0; j < ring_size; j++) {
                HANDLE_OR_RETURN(this->child_.child_.read_coord(handler, initial_coord_offset + j, j));
            }
            HANDLE_OR_RETURN(handler->ring_end(&this->meta_, ring_size, i, handler->handler_data));
        }

        HANDLE_OR_RETURN(handler->geometry_end(&this->meta_, part_id, handler->handler_data));
        return WK_CONTINUE;
    }

};


template <class ChildView>
class GeoArrowMultiView: public ListView<ChildView> {
  public:
    GeoArrowMultiView(struct ArrowSchema* schema): ListView<ChildView>(schema) {
        switch (this->child_.meta_.geometry_type) {
        case WK_POINT:
            this->meta_.geometry_type = WK_MULTIPOINT;
            this->vector_meta_.geometry_type = WK_MULTIPOINT;
            break;
        case WK_LINESTRING:
            this->meta_.geometry_type = WK_MULTILINESTRING;
            this->vector_meta_.geometry_type = WK_MULTILINESTRING;
            break;
        case WK_POLYGON:
            this->meta_.geometry_type = WK_MULTIPOLYGON;
            this->vector_meta_.geometry_type = WK_MULTIPOLYGON;
            break;
        default:
            this->meta_.geometry_type = WK_GEOMETRYCOLLECTION;
            this->vector_meta_.geometry_type = WK_GEOMETRYCOLLECTION;
            break;
        }

        if (this->child_.meta_.flags & WK_FLAG_HAS_Z) {
            this->meta_.flags |= WK_FLAG_HAS_Z;
            this->vector_meta_.flags |= WK_FLAG_HAS_Z;
        }

        if (this->child_.meta_.flags & WK_FLAG_HAS_M) {
            this->meta_.flags |= WK_FLAG_HAS_M;
            this->vector_meta_.flags |= WK_FLAG_HAS_M;
        }
    }

    int read_features(wk_handler_t* handler) {
        return read_features_templ<GeoArrowMultiView>(*this, handler);
    }

    int read_feature(wk_handler_t* handler, int64_t offset) {
        return read_feature_templ<GeoArrowMultiView>(*this, offset, handler);
    }

    int read_geometry(wk_handler_t* handler, int64_t offset, uint32_t part_id = WK_PART_ID_NONE) {
        int result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);
        this->meta_.size = size;

        HANDLE_OR_RETURN(handler->geometry_start(&this->meta_, part_id, handler->handler_data));
        for (int64_t i = 0; i < size; i++) {
            HANDLE_OR_RETURN(this->child_.read_geometry(handler, initial_child_offset + i, i));
        }
        HANDLE_OR_RETURN(handler->geometry_end(&this->meta_, part_id, handler->handler_data));
        return WK_CONTINUE;
    }
};

}; // namespace geoarrow
