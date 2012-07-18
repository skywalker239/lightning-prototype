#pragma once

#include "configuration_store.h"
#include <mordor/fibersynchronization.h>
#include <string>

namespace lightning {

//! A temporary implementation that maintains the map in memory
//  reading the initial values from a config file.
//  TODO(skywalker): Replace with a Zookeeper-backed implementation.
class FileConfigurationStore : public ConfigurationStore {
public:
    FileConfigurationStore(const std::string& file);
    
    virtual bool get(const std::string& key,
                     std::string* value,
                     Version callerVersion,
                     Version* storeVersion) const;

    virtual bool set(const std::string& key,
                     const std::string& value,
                     Version replaceVersion,
                     Version* newVersion);
private:
    std::map<std::string, std::string> map_;
    uint64_t currentVersion_;

    mutable Mordor::FiberMutex mutex_;
};

}  // namespace lightning
