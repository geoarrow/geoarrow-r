#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <vector>

#include "wk-v1.h"
#include "narrow.h"
#include "geoarrow.h"
#include "util.h"

#define HANDLE_CONTINUE_OR_BREAK(expr)                         \
    result = expr;                                             \
    if (result == geoarrow::Handler::Result::ABORT_FEATURE) \
        continue; \
    else if (result == geoarrow::Handler::Result::ABORT) break


class WKGeoArrowHandler: public geoarrow::Handler {
public:

    WKGeoArrowHandler(wk_handler_t* handler, R_xlen_t size):
      handler_(handler), feat_id_(-1), ring_id_(-1),
      coord_id_(-1) {
        WK_VECTOR_META_RESET(vector_meta_, WK_GEOMETRY);
        WK_META_RESET(meta_, WK_GEOMETRY);

        vector_meta_.size = size;

        // This is to keep vectors from being reallocated, since some
        // wk handlers assume that the meta pointers will stay valid between
        // the start and end geometry methods (this will get fixed in a
        // wk release soon)
        part_id_stack_.reserve(32);
        meta_stack_.reserve(32);
    }

    void new_geometry_type(geoarrow::util::GeometryType geometry_type) {
        if (geometry_type == geoarrow::util::GeometryType::GEOMETRY_TYPE_UNKNOWN) {
            vector_meta_.geometry_type = WK_GEOMETRY;
        } else {
            vector_meta_.geometry_type = geometry_type;
        }
    }

    void new_dimensions(geoarrow::util::Dimensions dimensions) {
        vector_meta_.flags &= ~WK_FLAG_HAS_Z;
        vector_meta_.flags &= ~WK_FLAG_HAS_M;
        meta_.flags &= ~WK_FLAG_HAS_Z;
        meta_.flags &= ~WK_FLAG_HAS_M;

        switch (dimensions) {
        case geoarrow::util::Dimensions::XYZ:
        case geoarrow::util::Dimensions::XYZM:
            vector_meta_.flags |= WK_FLAG_HAS_Z;
            meta_.flags |= WK_FLAG_HAS_Z;
            break;
        default:
            break;
        }

        switch (dimensions) {
        case geoarrow::util::Dimensions::XYM:
        case geoarrow::util::Dimensions::XYZM:
            vector_meta_.flags |= WK_FLAG_HAS_M;
            meta_.flags |= WK_FLAG_HAS_M;
            break;
        default:
            break;
        }

        if (dimensions == geoarrow::util::Dimensions::DIMENSIONS_UNKNOWN) {
            vector_meta_.flags |= WK_FLAG_DIMS_UNKNOWN;
        } else {
            vector_meta_.flags &= ~WK_FLAG_DIMS_UNKNOWN;
        }
    }

    Result feat_start() {
        feat_id_++;
        part_id_stack_.clear();
        meta_stack_.clear();
        return (Result) handler_->feature_start(&vector_meta_, feat_id_, handler_->handler_data);
    }

    Result null_feat() {
        return (Result) handler_->null_feature(handler_->handler_data);
    }

    Result geom_start(geoarrow::util::GeometryType geometry_type, int32_t size) {
        ring_id_ = -1;
        coord_id_ = -1;

        if (part_id_stack_.size() > 0) {
            part_id_stack_[part_id_stack_.size() - 1]++;
        }

        meta_.geometry_type = geometry_type;
        meta_.size = size;
        meta_stack_.push_back(meta_);

        Result result = (Result) handler_->geometry_start(meta(), part_id(), handler_->handler_data);
        part_id_stack_.push_back(-1);
        return result;
    }

    Result ring_start(int32_t size) {
        ring_id_++;
        coord_id_ = -1;
        ring_size_ = size;
        return (Result) handler_->ring_start(meta(), ring_size_, ring_id_, handler_->handler_data);
    }

    Result coords(const double* coord, int64_t n, int32_t coord_size) {
        int result;
        for (int64_t i = 0; i < n; i++) {
            coord_id_++;
            result = handler_->coord(meta(), coord + (i * coord_size), coord_id_, handler_->handler_data);
            if (result != WK_CONTINUE) {
                return (Result) result;
            }
        }

        return Result::CONTINUE;
    }

    Result ring_end() {
        return (Result) handler_->ring_end(meta(), ring_size_, ring_id_, handler_->handler_data);
    }

    Result geom_end() {
        if (part_id_stack_.size() > 0) part_id_stack_.pop_back();
        int result = handler_->geometry_end(meta(), part_id(), handler_->handler_data);
        if (meta_stack_.size() > 0) meta_stack_.pop_back();
        return (Result) result;
    }

    Result feat_end() {
        return (Result) handler_->feature_end(&vector_meta_, feat_id_, handler_->handler_data);
    }

    wk_vector_meta_t vector_meta_;

private:
    wk_handler_t* handler_;

    std::vector<wk_meta_t> meta_stack_;
    std::vector<int32_t> part_id_stack_;
    wk_meta_t meta_;

    int32_t ring_size_;
    int64_t feat_id_;

    int32_t ring_id_;
    int32_t coord_id_;

    int32_t part_id() {
        if (part_id_stack_.size() == 0) {
            return WK_PART_ID_NONE;
        } else {
            return part_id_stack_[part_id_stack_.size() - 1];
        }
    }

    const wk_meta_t* meta() {
        if (meta_stack_.size() == 0) {
            throw std::runtime_error("geom_start()/geom_end() stack imbalance <meta>");
        }
        return meta_stack_.data() + meta_stack_.size() - 1;
    }
};

SEXP geoarrow_handle_stream(SEXP data, wk_handler_t* handler) {
    CPP_START

    struct ArrowArrayStream* array_stream = array_stream_from_xptr(VECTOR_ELT(data, 0), "handleable");
    struct ArrowSchema* schema = schema_from_xptr(VECTOR_ELT(data, 1), "schema");
    SEXP n_features_sexp = VECTOR_ELT(data, 2);

    // We can't stack allocate this because we don't know the exact type that is returned
    // and we can't rely on the deleter to run because one of the handler
    // calls could longjmp. We use the same trick for making sure the array_data is
    // released for each array in the stream.
    geoarrow::ArrayView* view = geoarrow::create_view(schema);
    SEXP view_xptr = PROTECT(R_MakeExternalPtr(view, R_NilValue, R_NilValue));
    R_RegisterCFinalizer(view_xptr, &delete_array_view_xptr);

    R_xlen_t vector_size = WK_VECTOR_SIZE_UNKNOWN;
    if (TYPEOF(n_features_sexp) == INTSXP) {
        if (INTEGER(n_features_sexp)[0] != NA_INTEGER) {
            vector_size = INTEGER(n_features_sexp)[0];
        }
    } else {
        double n_features_double = REAL(n_features_sexp)[0];
        if (!ISNA(n_features_double) && !ISNAN(n_features_double)) {
            vector_size = n_features_double;
        }
    }

    WKGeoArrowHandler geoarrow_handler(handler, vector_size);
    view->read_meta(&geoarrow_handler);

    int result = handler->vector_start(&geoarrow_handler.vector_meta_, handler->handler_data);
    if (result == WK_CONTINUE) {
        struct ArrowArray* array_data = (struct ArrowArray*) malloc(sizeof(struct ArrowArray));
        if (array_data == NULL) {
            Rf_error("Failed to allocate struct ArrowArray");
        }
        array_data->release = NULL;
        SEXP array_data_wrapper = PROTECT(R_MakeExternalPtr(array_data, R_NilValue, R_NilValue));
        R_RegisterCFinalizer(array_data_wrapper, &geoarrow_finalize_array_data);

        int stream_result = 0;
        while(result != WK_ABORT) {
            if (array_data->release != NULL) {
                array_data->release(array_data);
            }
            stream_result = array_stream->get_next(array_stream, array_data);
            if (stream_result != 0) {
                const char* error_message = array_stream->get_last_error(array_stream);
                if (error_message != NULL) {
                    Rf_error("[%d] %s", stream_result, error_message);
                } else {
                    Rf_error("ArrowArrayStream->get_next() failed with code %d", stream_result);
                }
            }

            if (array_data->release == NULL) {
                break;
            }

            view->set_array(array_data);
            result = (int) view->read_features(&geoarrow_handler);
            if (result == WK_CONTINUE) {
                continue;
            } else if (result == WK_ABORT) {
                break;
            }
        }

        UNPROTECT(1);
    }

    SEXP result_sexp = PROTECT(handler->vector_end(&geoarrow_handler.vector_meta_, handler->handler_data));
    UNPROTECT(2);
    return result_sexp;

    CPP_END
}

SEXP geoarrow_handle_vctr(SEXP data, wk_handler_t* handler) {
    CPP_START
    if (TYPEOF(data) != INTSXP) {
        Rf_error("geoarrow_vctr index type not supported");
    }

    struct ArrowSchema* schema = schema_from_xptr(Rf_getAttrib(data, Rf_install("schema")), "");
    SEXP array_data_sexp = Rf_getAttrib(data, Rf_install("array_data"));
    int n_array_data = Rf_length(array_data_sexp);

    SEXP array_data_offsets_sexp = PROTECT(Rf_allocVector(INTSXP, n_array_data + 1));
    SEXP array_data_pointers_sexp = PROTECT(Rf_allocVector(RAWSXP, n_array_data * sizeof(void*)));
    int* array_data_offsets = INTEGER(array_data_offsets_sexp);
    struct ArrowArray** array_data_pointers =
        reinterpret_cast<struct ArrowArray**>(DATAPTR(array_data_pointers_sexp));

    array_data_offsets[0] = 0;
    for (int j = 0; j < n_array_data; j++) {
        array_data_pointers[j] = array_data_from_xptr(VECTOR_ELT(array_data_sexp, j), "");
        array_data_offsets[j + 1] = array_data_offsets[j] + array_data_pointers[j]->length;
    }

    geoarrow::ArrayView* view = geoarrow::create_view(schema);
    SEXP view_xptr = PROTECT(R_MakeExternalPtr(view, R_NilValue, R_NilValue));
    R_RegisterCFinalizer(view_xptr, &delete_array_view_xptr);

    R_xlen_t vector_size = Rf_length(data);
    // Note: don't stack allocate this!
    WKGeoArrowHandler geoarrow_handler(handler, vector_size);
    view->read_meta(&geoarrow_handler);

    int result = handler->vector_start(&geoarrow_handler.vector_meta_, handler->handler_data);
    if (result == WK_CONTINUE) {
        int n = Rf_length(data);
        int buf[1024];
        int vctr_index;
        int array_index;
        geoarrow::Handler::Result result;

        for (int i = 0; i < n; i++) {
            if ((i % 1024) == 0) {
                INTEGER_GET_REGION(data, i, 1024, buf);
            }

            vctr_index = buf[i % 1024];

            if (vctr_index == NA_INTEGER || vctr_index < 1 || vctr_index > array_data_offsets[n_array_data]) {
                HANDLE_CONTINUE_OR_BREAK(geoarrow_handler.feat_start());
                HANDLE_CONTINUE_OR_BREAK(geoarrow_handler.null_feat());
                HANDLE_CONTINUE_OR_BREAK(geoarrow_handler.feat_end());
            } else {
                for (int j = 1; j <= n_array_data; j++) {
                    if (vctr_index <= array_data_offsets[j]) {
                        view->set_array(array_data_pointers[j - 1]);
                        array_index = vctr_index - array_data_offsets[j - 1] - 1;
                        break;
                    }
                }

                HANDLE_CONTINUE_OR_BREAK(view->read_feature(&geoarrow_handler, array_index));
            }

        }

    }

    SEXP result_sexp = PROTECT(handler->vector_end(&geoarrow_handler.vector_meta_, handler->handler_data));
    UNPROTECT(4);
    return result_sexp;

    CPP_END
}

extern "C" SEXP geoarrow_c_handle_stream(SEXP data, SEXP handler_xptr) {
  return wk_handler_run_xptr(&geoarrow_handle_stream, data, handler_xptr);
}

extern "C" SEXP geoarrow_c_handle_vctr(SEXP data, SEXP handler_xptr) {
  return wk_handler_run_xptr(&geoarrow_handle_vctr, data, handler_xptr);
}
