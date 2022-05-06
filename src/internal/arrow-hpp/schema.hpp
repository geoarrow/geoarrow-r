
#pragma once

#include <string>
#include <vector>

#include "common.hpp"

extern "C" void arrow_hpp_release_schema_internal(struct ArrowSchema* schema);

#ifdef ARROW_HPP_IMPL

// The .release() callback for all struct ArrowSchemas populated here
void arrow_hpp_release_schema_internal(struct ArrowSchema* schema) {
  if (schema != nullptr && schema->release != nullptr) {
    // format, name, and metadata must be nullptr or allocated with malloc()
    if (schema->format != nullptr) free((void*) schema->format);
    if (schema->name != nullptr) free((void*) schema->name);
    if (schema->metadata != nullptr) free((void*) schema->metadata);

    // this object owns the memory for all the children, but those
    // children may have been generated elsewhere and might have
    // their own release() callback.
    if (schema->children != nullptr) {
      for (int64_t i = 0; i < schema->n_children; i++) {
        if (schema->children[i] != nullptr) {
          if(schema->children[i]->release != nullptr) {
            schema->children[i]->release(schema->children[i]);
          }

          free(schema->children[i]);
        }
      }

      free(schema->children);
    }

    // this object owns the memory for the dictionary but it
    // may have been generated somewhere else and have its own
    // release() callback.
    if (schema->dictionary != nullptr) {
      if (schema->dictionary->release != nullptr) {
        schema->dictionary->release(schema->dictionary);
      }

      free(schema->dictionary);
    }

    // private data must be allocated with malloc() if needed
    if (schema->private_data != nullptr) {
      free(schema->private_data);
    }

    schema->release = nullptr;
  }
}
#endif

namespace arrow {

namespace hpp {

// Allocates a struct ArrowSchema whose members can be further
// populated by the caller. This ArrowSchema owns the memory of its children
// and its dictionary (i.e., the parent release() callback will call the
// release() method of each child and then free() it).
inline void allocate_schema(struct ArrowSchema* schema, int64_t n_children = 0) {
  // schema->name and/or schema->format must be allocated via malloc()
  schema->format = nullptr;
  schema->name = nullptr;
  schema->metadata = nullptr;
  schema->flags = ARROW_FLAG_NULLABLE;
  schema->n_children = n_children;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->private_data = nullptr;
  schema->release = &arrow_hpp_release_schema_internal;

  if (n_children > 0) {
    schema->children = reinterpret_cast<struct ArrowSchema**>(
      malloc(n_children * sizeof(struct ArrowSchema*)));

    if (schema->children == nullptr) {
      arrow_hpp_release_schema_internal(schema);
      throw util::Exception(
        "Failed to allocate schema->children of size %lld", n_children);
    }

    memset(schema->children, 0, n_children * sizeof(struct ArrowSchema*));

    for (int64_t i = 0; i < n_children; i++) {
      schema->children[i] = reinterpret_cast<struct ArrowSchema*>(
        malloc(sizeof(struct ArrowSchema)));

      if (schema->children[i] == nullptr) {
        arrow_hpp_release_schema_internal(schema);
        throw util::Exception("Failed to allocate schema->children[%lld]", i);
      }

      schema->children[i]->release = nullptr;
    }
  }

  // we don't allocate the dictionary because it has to be nullptr
  // for non-dictionary-encoded arrays
}

static inline int64_t schema_metadata_size(const char* metadata) {
  if (metadata == NULL) {
    return 0;
  }

  int64_t pos = 0;
  int32_t n;
  memcpy(&n, metadata + pos, sizeof(int32_t));
  pos += sizeof(int32_t);

  for (int i = 0; i < n; i++) {
    int32_t name_len;
    memcpy(&name_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (name_len > 0) {
      pos += name_len;
    }

    int32_t value_len;
    memcpy(&value_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (value_len > 0) {
      pos += value_len;
    }
  }

  return pos;
}

static inline std::vector<std::string> schema_metadata_names(const char* metadata) {
  std::vector<std::string> names;

  if (metadata == NULL) {
    return names;
  }

  int64_t pos = 0;
  int32_t n;
  memcpy(&n, metadata + pos, sizeof(int32_t));
  pos += sizeof(int32_t);

  for (int i = 0; i < n; i++) {
    int32_t name_len;
    memcpy(&name_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (name_len > 0) {
      names.push_back(std::string(metadata + pos, name_len));
      pos += name_len;
    } else {
      names.push_back("");
    }

    int32_t value_len;
    memcpy(&value_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (value_len > 0) {
      pos += value_len;
    }
  }

  return names;
}

static inline std::string schema_metadata_key(const char* metadata, const std::string& key,
                                              const std::string& default_value = "") {
  if (metadata == NULL) {
    return default_value;
  }

  int64_t pos = 0;
  int32_t n;
  memcpy(&n, metadata + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  std::string name("");
  std::string value = default_value;

  for (int i = 0; i < n; i++) {
    int32_t name_len;
    memcpy(&name_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (name_len > 0) {
      name = std::string(metadata + pos, name_len);
      pos += name_len;
    }

    int32_t value_len;
    memcpy(&value_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (value_len > 0) {
      value = std::string(metadata + pos, value_len);
      pos += value_len;
    } else {
      value = std::string("");
    }

    if (name == key) {
      return value;
    }
  }

  return default_value;
}

static inline char* schema_metadata_create(const std::vector<std::string>& names,
                                           const std::vector<std::string>& values) {
    if (names.size() != values.size()) {
        throw util::Exception("names.size() != values.size()");
    }

    // calculate how much space we need
    size_t total_size = sizeof(int32_t);

    for (const std::string& name: names) {
        total_size += sizeof(int32_t);
        total_size += name.size();
    }

    for (const std::string& value: values) {
        total_size += sizeof(int32_t);
        total_size += value.size();
    }

    // allocate!
    char* metadata = reinterpret_cast<char*>(malloc(total_size));
    if (metadata == nullptr) {
        throw util::Exception("malloc() of metadata failed");
    }

    // serialize the data
    int64_t pos = 0;
    int32_t len = names.size();
    memcpy(metadata + pos, &len, sizeof(int32_t));
    pos += sizeof(int32_t);

    for (size_t i = 0; i < names.size(); i++) {
        len = names[i].size();
        memcpy(metadata + pos, &len, sizeof(int32_t));
        pos += sizeof(int32_t);
        memcpy(metadata + pos, names[i].data(), names[i].size());
        pos += names[i].size();

        len = values[i].size();
        memcpy(metadata + pos, &len, sizeof(int32_t));
        pos += sizeof(int32_t);
        memcpy(metadata + pos, values[i].data(), values[i].size());
        pos += values[i].size();
    }

    return metadata;
}

class SchemaFinalizer {
public:
  struct ArrowSchema schema;

  SchemaFinalizer() {
    schema.release = nullptr;
  }

  void allocate(int64_t n_children = 0) {
    allocate_schema(&schema, n_children);
    set_format("");
    set_name("");
  }

  void set_format(const char* format) {
    if (schema.format != nullptr) {
      free((void*) schema.format);
    }

    size_t len = strlen(format);
    char* format_owned = reinterpret_cast<char*>(malloc(len + 1));
    if (format_owned == nullptr) {
      throw util::Exception("Failed to allocate schema format");
    }
    memcpy(format_owned, format, len);
    format_owned[len] = '\0';

    schema.format = format_owned;
  }

  void set_name(const char* name) {
    if (schema.name != nullptr) {
      free((void*) schema.name);
    }

    size_t len = strlen(name);
    char* name_owned = reinterpret_cast<char*>(malloc(len + 1));
    if (name_owned == nullptr) {
      throw util::Exception("Failed to allocate schema name");
    }
    memcpy(name_owned, name, len);
    name_owned[len] = '\0';

    schema.name = name_owned;
  }

  void set_metadata(const char* metadata) {
    int64_t metadata_size = schema_metadata_size(metadata);
    if (metadata_size > 0) {
      char* new_metadata = reinterpret_cast<char*>(malloc(metadata_size));
      memcpy(new_metadata, metadata, metadata_size);
      set_metadata_internal(new_metadata);
    } else {
      set_metadata_internal(nullptr);
    }
  }

  void set_metadata(const std::vector<std::string>& names,
                    const std::vector<std::string>& values) {
    set_metadata_internal(schema_metadata_create(names, values));
  }

  void release(struct ArrowSchema* schema_out) {
    if (schema_out == nullptr) {
      throw util::Exception("output schema is nullptr");
    }

    if (schema_out->release != nullptr) {
      schema_out->release(schema_out);
    }

    memcpy(schema_out, &schema, sizeof(struct ArrowSchema));
    schema.release = nullptr;
  }

  ~SchemaFinalizer() {
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
  }

private:
  void set_metadata_internal(char* metadata) {
    if (schema.metadata != nullptr) {
      free((void*) schema.metadata);
      schema.metadata = nullptr;
    }

    schema.metadata = metadata;
  }
};

static inline void schema_deep_copy(struct ArrowSchema* schema_in, struct ArrowSchema* schema_out) {
  SchemaFinalizer finalizer;
  finalizer.allocate(schema_in->n_children);
  finalizer.set_format(schema_in->format);
  finalizer.set_name(schema_in->name);
  finalizer.set_metadata(schema_in->metadata);

  for (int64_t i = 0; i < schema_in->n_children; i++) {
    schema_deep_copy(schema_in->children[i], finalizer.schema.children[i]);
  }

  if (schema_in->dictionary != nullptr) {
    finalizer.schema.dictionary = reinterpret_cast<struct ArrowSchema*>(malloc(sizeof(struct ArrowSchema)));
    if (finalizer.schema.dictionary == nullptr) {
      throw util::Exception("Failed malloc (struct ArrowSchema*)");
    }

    schema_in->release = nullptr;
    schema_deep_copy(schema_in->dictionary, finalizer.schema.dictionary);
  }

  finalizer.release(schema_out);
}

}

}
