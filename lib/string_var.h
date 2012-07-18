#pragma once

#include "configuration_var_base.h"

namespace lightning {

class StringVar : public ConfigurationVarBase {
public:
    StringVar(const std::string& key,
              ConfigurationStore::ptr store);

    const std::string& get() const;

    void reset(const std::string& newValue);
};

}  // namespace lightning
