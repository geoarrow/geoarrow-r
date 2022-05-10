
#pragma once

#include "meta.hpp"
#include "handler.hpp"
#include "array-view-base.hpp"

namespace geoarrow {

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
        return internal::read_features_templ<PointArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return internal::read_feature_templ<PointArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        Handler::Result result;
        HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POINT, 1));
        HANDLE_OR_RETURN(read_coords(handler, offset, 1));
        HANDLE_OR_RETURN(handler->geom_end());
        return Handler::Result::CONTINUE;
    }

    Handler::Result read_coords(Handler* handler, int64_t offset, int64_t n) {
        Handler::Result result;
        HANDLE_OR_RETURN(handler->coords(data_buffer_ + (offset + array_->offset) * coord_size_, n, coord_size_));
        return Handler::Result::CONTINUE;
    }

    int coord_size_;
    const double* data_buffer_;
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


class LinestringArrayView: public ListArrayView<PointArrayView> {
  public:
    LinestringArrayView(struct ArrowSchema* schema): ListArrayView<PointArrayView>(schema) {}

    Handler::Result read_features(Handler* handler) {
        return internal::read_features_templ<LinestringArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return internal::read_feature_templ<LinestringArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        Handler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::LINESTRING, size));
        HANDLE_OR_RETURN(this->child_.read_coords(handler, initial_child_offset, size));
        HANDLE_OR_RETURN(handler->geom_end());
        return Handler::Result::CONTINUE;
    }
};


class PolygonArrayView: public ListArrayView<ListArrayView<PointArrayView>> {
  public:
    PolygonArrayView(struct ArrowSchema* schema): ListArrayView<ListArrayView<PointArrayView>>(schema) {}

    Handler::Result read_features(Handler* handler) {
        return internal::read_features_templ<PolygonArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return internal::read_feature_templ<PolygonArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        Handler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(util::GeometryType::POLYGON, size));
        for (int64_t i = 0; i < size; i++) {

            int64_t initial_coord_offset = this->child_.child_offset(initial_child_offset + i);
            int64_t ring_size = this->child_.child_size(initial_child_offset + i);

            HANDLE_OR_RETURN(handler->ring_start(ring_size));
            HANDLE_OR_RETURN(this->child_.child_.read_coords(handler, initial_coord_offset, ring_size));
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
        return internal::read_features_templ<CollectionArrayView>(*this, handler);
    }

    Handler::Result read_feature(Handler* handler, int64_t offset) {
        return internal::read_feature_templ<CollectionArrayView>(*this, offset, handler);
    }

    Handler::Result read_geometry(Handler* handler, int64_t offset) {
        Handler::Result result;

        int64_t initial_child_offset = this->child_offset(offset);
        int64_t size = this->child_size(offset);

        HANDLE_OR_RETURN(handler->geom_start(this->meta_.geometry_type_, size));
        for (int64_t i = 0; i < size; i++) {
            HANDLE_OR_RETURN(this->child_.read_geometry(handler, initial_child_offset + i));
        }
        HANDLE_OR_RETURN(handler->geom_end());
        return Handler::Result::CONTINUE;
    }
};

}
