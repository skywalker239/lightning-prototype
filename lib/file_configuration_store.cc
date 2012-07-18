#include "file_configuration_store.h"
#include <mordor/exception.h>
#include <mordor/log.h>
#include <fstream>

namespace lightning {

using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using std::map;
using std::ifstream;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:file_configuration_store");

FileConfigurationStore::FileConfigurationStore(const std::string& file)
    : currentVersion_(0)
{
    ifstream in(file.c_str());
    size_t lines;
    in >> lines;
    for(size_t i = 0; i < lines; ++i) {
        string key;
        in >> key;
        string value;
        in >> value;
        if(!in) {
            MORDOR_LOG_ERROR(g_log) << this <<
                " cannot read next line from " << file;
            MORDOR_THROW_EXCEPTION_FROM_LAST_ERROR();
        }

        MORDOR_LOG_TRACE(g_log) << this << " init " << key << " -> " << value;
    }
}

bool FileConfigurationStore::get(const string& key,
                                 string* value,
                                 Version callerVersion,
                                 Version* storeVersion) const
{
    FiberMutex::ScopedLock lk(mutex_);

    if((callerVersion < currentVersion_) ||
       (callerVersion == kAnyVersion))
    {
        *storeVersion = currentVersion_;
        map<string, string>::const_iterator valueIter = map_.find(key);
        if(valueIter == map_.end()) {
            *value = "";
        } else {
            *value = valueIter->second;
        }
        MORDOR_LOG_TRACE(g_log) << this << " get(" << key << ", " <<
            callerVersion << ") = true(" << *value << ", " << *storeVersion << ")";
        return true;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " get(" << key << ", " <<
            callerVersion << ") = false (" << currentVersion_ << ")";
        return false;
    }
}

bool FileConfigurationStore::set(const string& key,
                                 const string& value,
                                 Version replaceVersion,
                                 Version* newVersion)
{
    FiberMutex::ScopedLock lk(mutex_);

    if((replaceVersion == currentVersion_) ||
       (replaceVersion == kAnyVersion))
    {
        map_[key] = value;
        ++currentVersion_;
        *newVersion = currentVersion_;
        MORDOR_LOG_TRACE(g_log) << this << " set(" << key << ", " << value <<
            replaceVersion << ") = true(" << *newVersion << ")";
        return true;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " set(" << key << ", " << value <<
            replaceVersion << ") = false(" << currentVersion_ << ")";
        return false;
    }
}

}  // namespace lightning
