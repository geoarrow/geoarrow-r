
#include "carrow.h"
#include "wk-v1.h"
#include <memory>
#include <cstring>
#include <algorithm>

#define HANDLE_OR_RETURN(expr)                                 \
    result = expr;                                             \
    if (result != WK_CONTINUE) return result


#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == WK_ABORT_FEATURE) continue; else if (result == WK_ABORT) break


template <class ArrayView>
int read_point_geometry(ArrayView& view, wk_handler_t* handler, int64_t part_id = WK_PART_ID_NONE) {
    int result = handler->geometry_start(&view.meta_, part_id, handler->handler_data);
    if (result != WK_CONTINUE) {
        view.offset_++;
        return result;
    }
    HANDLE_OR_RETURN(view.read_coord(handler, 0));
    HANDLE_OR_RETURN(handler->geometry_end(&view.meta_, part_id, handler->handler_data));
    return WK_CONTINUE;
}


template <class ArrayView>
int read_point_feature(ArrayView& view, wk_handler_t* handler) {
    int result;
    view.feature_id_++;
    result = handler->feature_start(&view.vector_meta_, view.feature_id_, handler->handler_data);
    if (result != WK_CONTINUE) {
        view.offset_++;
        return result;
    }

    if (view.is_null(1)) {
        view.offset_++;
        HANDLE_OR_RETURN(handler->null_feature(handler->handler_data));
    } else {
        HANDLE_OR_RETURN(read_point_geometry<ArrayView>(view, handler, WK_PART_ID_NONE));
    }

    HANDLE_OR_RETURN(handler->feature_end(&view.vector_meta_, view.feature_id_, handler->handler_data));
    return WK_CONTINUE;
}


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

    GeoArrowMeta(const struct ArrowSchema* schema = nullptr) {
        if (!set_schema(schema) && schema != nullptr) {
            throw std::runtime_error(error_);
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
            return false;
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

            if (name_len >= 20 && strncmp(name, "ARROW:extension:name", 24) == 0) {
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


class GeoArrowArrayView {
  public:
    GeoArrowArrayView(const struct ArrowSchema* schema): 
      schema_(schema), array_(nullptr), geoarrow_meta_(schema),
      offset_(-1), feature_id_(-1), validity_buffer_(nullptr) {
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

    void set_array(struct ArrowArray* array) {
        array_ = array;
        offset_ = -1;
        validity_buffer_ = reinterpret_cast<const uint8_t*>(array->buffers[0]);
    }

    void set_vector_size(int64_t size) {
        vector_meta_.size = size;
    }

    bool is_null(int64_t delta = 0) {
        return false;
    }


    wk_meta_t meta_;
    wk_vector_meta_t vector_meta_;
    const struct ArrowSchema* schema_;
    const struct ArrowArray* array_;
    GeoArrowMeta geoarrow_meta_;
    int64_t offset_;
    int64_t feature_id_;
    const uint8_t* validity_buffer_;
};


class GeoArrowPointView: public GeoArrowArrayView {
  public:    
    GeoArrowPointView(struct ArrowSchema* schema): 
      GeoArrowArrayView(schema), data_buffer_(nullptr) {
        meta_.geometry_type = WK_POINT;
        vector_meta_.geometry_type = WK_POINT;
        meta_.size = 1;

        coord_size_ = 2;
        if (vector_meta_.flags & WK_FLAG_HAS_Z) coord_size_++;
        if (vector_meta_.flags & WK_FLAG_HAS_M) coord_size_++;
    }

    void set_array(struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        data_buffer_ = reinterpret_cast<const double*>(array->children[0]->buffers[1]);
    }

    int read_feature(wk_handler_t* handler) {
        return read_point_feature<GeoArrowPointView>(*this, handler);
    }

    int read_geometry(wk_handler_t* handler, uint32_t part_id = WK_PART_ID_NONE) {
        return read_point_geometry<GeoArrowPointView>(*this, handler, part_id);
    }

    int read_coord(wk_handler_t* handler, int64_t coord_id = 0) {
        int result;
        offset_++;
        HANDLE_OR_RETURN(handler->coord(
            &meta_, 
            data_buffer_ + (offset_ + array_->offset) * coord_size_,
            coord_id,
            handler->handler_data));
        return WK_CONTINUE;
    }

    int coord_size_;
    const double* data_buffer_;
};


class GeoArrowPointStructView: public GeoArrowArrayView {
  public:    
    GeoArrowPointStructView(struct ArrowSchema* schema): GeoArrowArrayView(schema) {
        meta_.geometry_type = WK_POINT;
        meta_.size = 1;

        coord_size_ = 2;
        if (vector_meta_.flags & WK_FLAG_HAS_Z) coord_size_++;
        if (vector_meta_.flags & WK_FLAG_HAS_M) coord_size_++;
        memset(coord_buffer_, 0, sizeof(coord_buffer_));
    }

    void set_array(struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        memset(coord_buffer_, 0, sizeof(coord_buffer_));
        for (int i = 0; i < coord_size_; i++) {
            const void* buffer_void = array->children[i]->buffers[1];
            coord_buffer_[i] = reinterpret_cast<const double*>(buffer_void);
        }
    }

    int read_feature(wk_handler_t* handler) {
        return read_point_feature<GeoArrowPointStructView>(*this, handler);
    }

    int read_geometry(wk_handler_t* handler, uint32_t part_id = WK_PART_ID_NONE) {
        return read_point_geometry<GeoArrowPointStructView>(*this, handler, part_id);
    }

    int read_coord(wk_handler_t* handler, int64_t coord_id = 0) {
        int result;
        offset_++;

        for (int i = 0; i < coord_size_; i++) {
            coord_[i] = coord_buffer_[i][array_->offset + offset_];
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
class FixedWidthListView: public GeoArrowArrayView {
  public:
    FixedWidthListView(struct ArrowSchema* schema): 
      GeoArrowArrayView(schema), ChildView(schema->children[0]) {
          width_ = atoi(schema->format + 3);
      }

    void set_array(struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        child_.set_array(array->children[0]);
    }

    int64_t child_offset(int32_t delta = 0) {
        return (array_->offset + offset_ + delta) * width_;
    }

    int64_t child_size(int32_t delta = 0) {
        return child_offset(delta + 1) - child_offset(delta);
    }

    ChildView child_;
    int32_t width_;
};


template <class ChildView, class offset_buffer_t = int32_t>
class ListView: public GeoArrowArrayView {
  public:
    ListView(struct ArrowSchema* schema): 
      GeoArrowArrayView(schema), ChildView(schema->children[0]), offset_buffer_(nullptr) {}

    void set_array(struct ArrowArray* array) {
        GeoArrowArrayView::set_array(array);
        child_.set_array(array->children[0]);
        offset_buffer_ = reinterpret_cast<const offset_buffer_t*>(array->buffers[1]);
    }

    int64_t child_offset(int32_t delta = 0) {
        return offset_buffer_[array_->offset + offset_ + delta];
    }

    int64_t child_size(int32_t delta = 0) {
        return child_offset(delta + 1) - child_offset(delta);
    }

    ChildView child_;
    const offset_buffer_t* offset_buffer_;
};


template <class PointView = GeoArrowPointView, class CoordContainerView = ListView<PointView>>
class GeoArrowLinestringView: public CoordContainerView {
    GeoArrowLinestringView(struct ArrowSchema* schema): CoordContainerView(schema) {}


};


template <class PointView = GeoArrowPointView, 
          class CoordContainerView = ListView<PointView>, 
          class RingContainerView = ListView<CoordContainerView>>
class GeoArrowPolygonView: public RingContainerView {
    GeoArrowPolygonView(struct ArrowSchema* schema): RingContainerView(schema) {}
};


template <class ChildView, class ChildContainerView = ListView<ChildView>>
class GeoArrowMultiView: public ChildContainerView {
    GeoArrowMultiView(struct ArrowSchema* schema): ChildContainerView(schema) {}
};





