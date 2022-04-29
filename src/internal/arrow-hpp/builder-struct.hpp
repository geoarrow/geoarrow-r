
#pragma once

#include <memory>
#include <vector>

#include "builder.hpp"

namespace arrow {

namespace hpp {

namespace builder {

class StructArrayBuilder: public ArrayBuilder {
public:
  StructArrayBuilder() {}

  void add_child(std::unique_ptr<ArrayBuilder> child, const std::string& name = "") {
    set_size(child->size());
    child->set_name(name);
    children_.push_back(std::move(child));
  }

  int64_t num_children() { return children_.size(); }

  void set_size(int64_t size) {
    if (num_children() > 0 && size != size_) {
      throw util::Exception(
        "Attempt to resize a StructArrayBuilder from %lld to %lld",
        size_, size);
    }

    size_ = size;
  }

  void shrink() {
    ArrayBuilder::shrink();
    for (int64_t i = 0; i < num_children(); i++) {
      children_[i]->shrink();
    }
  }

  void reserve(int64_t additional_capacity) {
    ArrayBuilder::reserve(additional_capacity);
    for (auto& child: children_) {
      child->reserve(additional_capacity);
    }
  }

  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    CArrayFinalizer finalizer;
    finalizer.allocate(1, num_children());
    finalizer.set_schema_format("+s");
    finalizer.set_schema_name(name().c_str());
    finalizer.set_schema_metadata(metadata_names_, metadata_values_);

    finalizer.array_data.length = size();
    finalizer.array_data.null_count = validity_buffer_builder_.null_count();

    for (int64_t i = 0; i < num_children(); i++) {
      children_[i]->release(
        finalizer.array_data.children[i],
        finalizer.schema->children[i]);
    }

    finalizer.release(array_data, schema);
  }

private:
  std::vector<std::unique_ptr<ArrayBuilder>> children_;
};

}

}

}
