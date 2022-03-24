
#pragma once

#include <cstdlib>
#include <limits>
#include <cstring>
#include <algorithm>
#include <vector>

#include "handler.hpp"
#include "common.hpp"
#include "../arrow-hpp/builder.hpp"

namespace geoarrow {

class ComputeOptions {
public:
  void set_bool(const std::string& key, bool value) {
    Item item;
    item.type_ = Type::BOOL;
    item.bool_ = value;
    set_item(key, std::move(item));
  }

  bool get_bool(const std::string& key) const {
    const Item& item = get_item(key);
    switch (item.type_) {
    case Type::BOOL: return item.bool_;
    default: throw util::IOException("Can't coerce key '%s' to BOOL", key.c_str());
    }
  }

  void set_schema(const std::string& key, struct ArrowSchema* value) {
    Item item;
    item.type_ = Type::SCHEMA;
    item.schema_ = value;
    set_item(key, std::move(item));
  }

  struct ArrowSchema* get_schema(const std::string& key) const {
    const Item& item = get_item(key);
    switch (item.type_) {
    case Type::SCHEMA: return item.schema_;
    default: throw util::IOException("Can't coerce key '%s' to SCHEMA", key.c_str());
    }
  }

private:
  enum Type {
    BOOL,
    SCHEMA
  };

  class Item {
  public:
    Type type_;
    bool bool_;
    struct ArrowSchema* schema_;
  };

  void set_item(const std::string& key, const Item& item) {
    names_.push_back(key);
    values_.push_back(std::move(item));
  }

  const Item& get_item(const std::string& key) const {
    for (int64_t i = names_.size() - 1; i >= 0; i--) {
      if (key == names_[i]) {
        return values_[i];
      }
    }

    throw util::IOException("No such key '%s'", key.c_str());
  }

  std::vector<std::string> names_;
  std::vector<Item> values_;
};

class ComputeBuilder: public arrow::hpp::builder::ArrayBuilder, public Handler {
public:
  ComputeBuilder(const ComputeOptions& options = ComputeOptions()): options_(std::move(options)) {}
  ComputeOptions* mutable_options() { return &options_; }
  const ComputeOptions& options() { return options_; }

private:
  ComputeOptions options_;
};

class NullBuilder: public ComputeBuilder {
public:

  void release(struct ArrowArray* array_data, struct ArrowSchema* schema) {
    arrow::hpp::builder::CArrayFinalizer finalizer;
    finalizer.allocate(0);
    finalizer.set_schema_format("n");
    finalizer.array_data.null_count = 0;
    finalizer.release(array_data, schema);
  }
};

}
