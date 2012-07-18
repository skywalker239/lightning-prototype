#include "string_var.h"

namespace lightning {

using std::string;

StringVar::StringVar(const string& key,
                     ConfigurationStore::ptr store)
    : ConfigurationVarBase(key, store)
{}

const string& StringVar::get() const {
    return value_;
}

}  // namespace lightning
