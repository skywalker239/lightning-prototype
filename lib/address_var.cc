#include "address_var.h"

namespace lightning {

using Mordor::Address;
using std::string;

AddressVar::AddressVar(const string& key,
                       ConfigurationStore::ptr store)
    : ConfigurationVarBase(key, store)
{}

Address::ptr AddressVar::get() const {
    return address_;
}

void AddressVar::updateImpl() {
    address_ = Address::lookup(value_).front();
}

}  // namespace lightning
