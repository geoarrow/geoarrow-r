
#include "geoarrow-meta.hpp"
#include "narrow.h"
#include "wk-v1.h"


#define HANDLE_OR_RETURN(expr)                                 \
    result = expr;                                             \
    if (result != WK_CONTINUE) return result


#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == WK_ABORT_FEATURE) continue; else if (result == WK_ABORT) break


namespace geoarrow {


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
            (validity_buffer_[offset / 8] << (offset % 8)) == 0;
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
