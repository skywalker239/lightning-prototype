#pragma once

#include <boost/shared_ptr.hpp>
#include <stdint.h>
#include <string>

namespace lightning {

//! A versioned string -> string map.
class ConfigurationStore {
public:
    typedef boost::shared_ptr<ConfigurationStore> ptr;

    typedef uint64_t Version;

    static const Version kAnyVersion = ~0ull;

    virtual ~ConfigurationStore() {}

    //! If the store's version is greater than callerVersion
    //  or callerVersion == kAnyVersion, then
    //  *value is set to the current value of key
    //  (or empty string if there is no such key),
    //  *storeVersion is set to the current store version and
    //  true is returned.
    //  Otherwise false is returned and *value and *storeVersion
    //  are unchanged.
    virtual bool get(const std::string& key,
                     std::string* value,
                     Version callerVersion,
                     Version *storeVersion) const = 0;

    //! If the store's current version is replaceVersion or
    //  replaceVersion is kAnyVersion, then the mapping for key
    //  is replaced by value, *newVersion is set to the new
    //  store version and true is returned.
    //  Otherwise, false is returned and *newVersion is unchanged.
    virtual bool set(const std::string& key,
                     const std::string& value,
                     Version replaceVersion,
                     Version* newVersion) = 0;
};

}  // namespace lightning
