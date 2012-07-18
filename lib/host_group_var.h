#include "configuration_var_base.h"
#include "host_configuration.h"
#include <vector>

namespace lightning {

//! Holds the (currently) static group of participating hosts.
class HostGroupVar : public ConfigurationVarBase {
public:
    HostGroupVar(const std::string& key,
                 ConfigurationStore::ptr store);

    size_t size() const;

    const HostConfiguration& host(size_t index) const;
private:
    virtual void updateImpl();

    std::vector<HostConfiguration> hosts_;
};

}  // namespace lightning
