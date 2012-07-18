#pragma once

#include "configuration_var_base.h"
#include <stdint.h>
#include <string>

namespace lightning {

class IntVar : public ConfigurationVarBase {
public:
    IntVar(const std::string& key,
           ConfigurationStore::ptr store);

    int64_t get() const;

    void reset(int64_t newValue);
protected:
    void updateImpl();
private:
    int64_t intValue_;
};

}  // namespace lightning
