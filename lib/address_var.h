#pragma once

#include "configuration_var_base.h"
#include <mordor/socket.h>

namespace lightning {

//! Holds a network address.
class AddressVar : public ConfigurationVarBase {
public:
    AddressVar(const std::string& key,
               ConfigurationStore::ptr store);

    Mordor::Address::ptr get() const;
private:
    void updateImpl();

    Mordor::Address::ptr address_;
};

}  // namespace lightning
