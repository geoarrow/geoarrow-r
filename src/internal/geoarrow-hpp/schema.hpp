
#pragma once

#include <vector>

#include "internal/arrow-hpp/schema.hpp"

namespace geoarrow {

class Metadata {
public:

    std::string build() {
        char* metadata = arrow::hpp::schema_metadata_create(names_, values_);
        std::string out(metadata, arrow::hpp::schema_metadata_size(metadata));
        free(metadata);
        return out;
    }

private:
    std::vector<std::string> names_;
    std::vector<std::string> values_;
};

}
