#pragma once

#include "configuration_store.h"
#include <string>

namespace lightning {

//! Watches a single configuration key.
//  Not fiber-safe.
class ConfigurationVarBase {
public:
    ConfigurationVarBase(const std::string& key,
                         ConfigurationStore::ptr store);

    virtual ~ConfigurationVarBase() {}

    //! Returns true if the value for the key has changed
    //  since the last update.
    bool update();

    //! Returns the value string (empty if never updated).
    const std::string& getString() const;

    //! Local version of the value. kAnyVersion if never updated.
    ConfigurationStore::Version localVersion() const;

protected:
    std::string value_;

    //! Called by update to parse the string representation
    //  into whatever the subclass wants.
    virtual void updateImpl() {}

    //! Unconditionally resets the variable in the store to
    //  the current local value.
    void resetString();

private:
    const std::string key_;
    ConfigurationStore::Version localVersion_;

    ConfigurationStore::ptr store_;
};

}  // namespace lightning
