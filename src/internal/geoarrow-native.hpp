
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
int read_point_feature(ArrayView& view, wk_handler_t* handler, int64_t part_id = WK_PART_ID_NONE) {
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
        result = handler->geometry_start(&view.meta_, part_id, handler->handler_data);
        if (result != WK_CONTINUE) {
            view.offset_++;
            return result;
        }
        HANDLE_OR_RETURN(view.read_coord(handler, 0));
        HANDLE_OR_RETURN(handler->geometry_end(&view.meta_, part_id, handler->handler_data));
    }

    HANDLE_OR_RETURN(handler->feature_end(&view.vector_meta_, view.feature_id_, handler->handler_data));
    return WK_CONTINUE;
}


class GeoArrowMeta {
  public:
    GeoArrowMeta(const char* metadata = nullptr): 
      geodesic_(false), crs_size_(0), crs_(nullptr) {
        memset(dim_, 0, sizeof(dim_));
        dim_[0] = 'x';
        dim_[1] = 'y';

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
            const char* name = reinterpret_cast<const char*>(metadata + pos);
            pos += name_len;

            memcpy(&value_len, metadata + pos, sizeof(int32_t));
            pos += sizeof(int32_t);

            if (name_len < 24 || strncmp(name, "ARROW:extension:metadata", 24) != 0) {
                pos += value_len;
                continue;
            }

            memcpy(&m, metadata + pos, sizeof(int32_t));
            pos += sizeof(int32_t);

            for (int j = 0; j < m; j++) {
                memcpy(&name_len, metadata + pos, sizeof(int32_t));
                pos += sizeof(int32_t);

                // !! not null-terminated!
                const char* name = reinterpret_cast<const char*>(metadata + pos);
                pos += name_len;

                memcpy(&value_len, metadata + pos, sizeof(int32_t));
                pos += sizeof(int32_t);

                // !! not null-terminated!
                const char* value = reinterpret_cast<const char*>(metadata + pos);
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
        }
    }

    char dim_[5];
    bool geodesic_;
    int32_t crs_size_;
    const char* crs_;
};


class GeoArrowArrayView {
  public:
    GeoArrowArrayView(const struct ArrowSchema* schema): 
      schema_(schema), array_(nullptr), geoarrow_meta_(schema->metadata),
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
    GeoArrowPointView(struct ArrowSchema* schema): GeoArrowArrayView(schema), data_buffer_(nullptr) {
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

    int read_feature(wk_handler_t* handler, int64_t part_id = WK_PART_ID_NONE) {
        return read_point_feature<GeoArrowPointView>(*this, handler, part_id);
    }

    int read_coord(wk_handler_t* handler, int64_t coord_id) {
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

    int read_feature(wk_handler_t* handler, int64_t part_id = WK_PART_ID_NONE) {
        return read_point_feature<GeoArrowPointStructView>(*this, handler, part_id);
    }

    int read_coord(wk_handler_t* handler, int64_t coord_id) {
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


template <class PointView>
struct GeoArrowLinestringView: public GeoArrowArrayView {
    PointView point;
};


template <class PointView>
struct GeoArrowPolygonView: public GeoArrowArrayView {
    PointView point;
};


template <class ChildView>
struct GeoArrowMultiView {
    ChildView child;
};
