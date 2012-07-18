#pragma once

#include "configuration_var_base.h"
#include <vector>

namespace lightning {

//! A ring configuration.
class RingVar : public ConfigurationVarBase {
public:
    RingVar(const std::string& key,
            ConfigurationStore::ptr store);

    bool valid() const;

    uint32_t ringId() const;

    const std::vector<uint32_t>& ringHostIds() const;
private:
    static const uint32_t kInvalidRingId = 0xFFFFFFFF;

    void updateImpl();

    uint32_t ringId_;
    std::vector<uint32_t> ringHostIds_;
};

}  // namespace lightning
