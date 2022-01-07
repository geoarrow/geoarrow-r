
#include "geoarrow-meta.hpp"
#include "carrow.h"
#include "wk-v1.h"

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





