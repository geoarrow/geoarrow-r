
#pragma once

#include "meta.hpp"
#include "handler.hpp"
#include "array-view-base.hpp"

#define HANDLE_OR_RETURN(expr)                                 \
    result = expr;                                             \
    if (result != Handler::Result::CONTINUE) return result

#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == Handler::Result::ABORT_FEATURE) \
        continue; \
    else if (result == Handler::Result::ABORT) break

namespace geoarrow {

namespace {

    template <class TArrayView>
    Handler::Result read_point_geometry(TArrayView& view, Handler* handler, int64_t offset) {
        Handler::Result result;
        HANDLE_OR_RETURN(handler->geom_start(1));
        HANDLE_OR_RETURN(view.read_coord(handler, offset));
        HANDLE_OR_RETURN(handler->geom_end());
        return Handler::Result::CONTINUE;
    }

    template <class TArrayView>
    Handler::Result read_feature_templ(TArrayView& view, int64_t offset, Handler* handler) {
        Handler::Result result;
        HANDLE_OR_RETURN(handler->feat_start());

        if (view.is_null(offset)) {
            HANDLE_OR_RETURN(handler->null_feat());
        } else {
            HANDLE_OR_RETURN(view.read_geometry(handler, offset));
        }

        HANDLE_OR_RETURN(handler->feat_end());
        return Handler::Result::CONTINUE;
    }

    template <class TArrayView>
    Handler::Result read_features_templ(TArrayView& view, Handler* handler) {
        Handler::Result result;

        for (uint64_t i = 0; i < view.array_->length; i++) {
            HANDLE_CONTINUE_OR_BREAK(view.read_feature(handler, i));
        }

        if (result == Handler::Result::ABORT) {
            return Handler::Result::ABORT;
        } else {
            return Handler::Result::CONTINUE;
        }
    }

} // anonymous namespace


class PointArrayView: public ArrayView {
  public:
    PointArrayView(const struct ArrowSchema* schema):
      ArrayView(schema), data_buffer_(nullptr) {
        coord_size_ = meta_.fixed_width_;
    }

    void set_array(const struct ArrowArray* array) {
        ArrayView::set_array(array);
        data_buffer_ = reinterpret_cast<const double*>(array->children[0]->buffers[1]);
    }

    Handler::Result read_features(Handler* handler) {
        return read_features_templ<PointArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return read_feature_templ<PointArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        return read_point_geometry<PointArrayView>(*this, handler, offset);
    }

    Handler::Result read_coord(Handler* handler, int64_t offset) {
        Handler::Result result;
        HANDLE_OR_RETURN(handler->coord(data_buffer_ + (offset + array_->offset) * coord_size_));
        return Handler::Result::CONTINUE;
    }

    int coord_size_;
    const double* data_buffer_;
};


class GeoArrowPointStructView: public ArrayView {
  public:
    GeoArrowPointStructView(const struct ArrowSchema* schema): ArrayView(schema) {
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
        ArrayView::set_array(array);
        memset(coord_buffer_, 0, sizeof(coord_buffer_));
        for (int i = 0; i < coord_size_; i++) {
            const void* buffer_void = array->children[i]->buffers[1];
            coord_buffer_[i] = reinterpret_cast<const double*>(buffer_void);
        }
    }

    Handler::Result read_features(Handler* handler) {
        return read_features_templ<GeoArrowPointStructView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return read_feature_templ<GeoArrowPointStructView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        return read_point_geometry<GeoArrowPointStructView>(*this, handler, offset);
    }

    Handler::Result read_coord(Handler* handler, int64_t offset) {
        Handler::Result result;

        for (int i = 0; i < coord_size_; i++) {
            coord_[i] = coord_buffer_[i][array_->offset + offset];
        }

        HANDLE_OR_RETURN(handler->coord(coord_));
        return Handler::Result::CONTINUE;
    }

    int coord_size_;
    double coord_[4];
    const double* coord_buffer_[4];
};


template <class ChildView>
class ListArrayView: public ArrayView {
  public:
    ListArrayView(const struct ArrowSchema* schema):
      ArrayView(schema), child_(schema->children[0]), offset_buffer_(nullptr) {}

    void set_array(const struct ArrowArray* array) {
        ArrayView::set_array(array);
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


template <class PointView = PointArrayView>
class LinestringArrayView: public ListArrayView<PointView> {
  public:
    LinestringArrayView(struct ArrowSchema* schema): ListArrayView<PointView>(schema) {}

    Handler::Result read_features(Handler* handler) {
        return read_features_templ<LinestringArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return read_feature_templ<LinestringArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        Handler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(size));
        for (int64_t i = 0; i < size; i++) {
            HANDLE_OR_RETURN(this->child_.read_coord(handler, initial_child_offset + i));
        }

        HANDLE_OR_RETURN(handler->geom_end());
        return Handler::Result::CONTINUE;
    }
};


template <class PointView = PointArrayView>
class PolygonArrayView: public ListArrayView<ListArrayView<PointView>> {
  public:
    PolygonArrayView(struct ArrowSchema* schema): ListArrayView<ListArrayView<PointView>>(schema) {}

    Handler::Result read_features(Handler* handler) {
        return read_features_templ<PolygonArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return read_feature_templ<PolygonArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        Handler::Result result;

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
        return Handler::Result::CONTINUE;
    }

};


template <class ChildView>
class CollectionArrayView: public ListArrayView<ChildView> {
  public:
    CollectionArrayView(struct ArrowSchema* schema): ListArrayView<ChildView>(schema) {}

    Handler::Result read_features(Handler* handler) {
        return read_features_templ<CollectionArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return read_feature_templ<CollectionArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        Handler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(size));
        for (int64_t i = 0; i < size; i++) {
            HANDLE_OR_RETURN(this->child_.read_geometry(handler, initial_child_offset + i));
        }
        HANDLE_OR_RETURN(handler->geom_end());
        return Handler::Result::CONTINUE;
    }
};

}

#undef HANDLE_OR_RETURN
#undef HANDLE_CONTINUE_OR_BREAK
