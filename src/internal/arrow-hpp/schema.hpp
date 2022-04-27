
#pragma once

#include <string>
#include <vector>

#include "common.hpp"

namespace arrow {

namespace hpp {

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

static inline std::string schema_metadata_key(const char* metadata, const std::string& key, int64_t max_size,
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
    if ((pos + sizeof(int32_t)) > max_size) {
      return default_value;
    }

    int32_t name_len;
    memcpy(&name_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (name_len > 0) {
      if ((pos + name_len) > max_size) {
          return default_value;
      }

      name = std::string(metadata + pos, name_len);
      pos += name_len;
    }

    if ((pos + sizeof(int32_t)) > max_size) {
      return default_value;
    }

    int32_t value_len;
    memcpy(&value_len, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (value_len > 0) {
      if ((pos + value_len) > max_size) {
        return default_value;
      }

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

}

}
