#include "configuration_var_base.h"

namespace lightning {

using std::string;

ConfigurationVarBase::ConfigurationVarBase(const string& key,
                                           ConfigurationStore::ptr store)
    : key_(key),
      localVersion_(ConfigurationStore::kAnyVersion),
      store_(store)
{}

bool ConfigurationVarBase::update() {
    string newValue;
    ConfigurationStore::Version storeVersion;
    if(store_->get(key_, &newValue, localVersion_, &storeVersion)) {
        value_ = newValue;
        localVersion_ = storeVersion;
        updateImpl();
        return true;
    }
    return false;
}

const string& ConfigurationVarBase::getString() const {
    return value_;
}

ConfigurationStore::Version ConfigurationVarBase::localVersion() const {
    return localVersion_;
}

}  // namespace lightning
