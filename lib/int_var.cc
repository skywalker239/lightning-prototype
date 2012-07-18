#include "int_var.h"
#include <boost/lexical_cast.hpp>
#include <mordor/log.h>

namespace lightning {

using Mordor::Log;
using Mordor::Logger;
using boost::lexical_cast;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:int_var");

IntVar::IntVar(const string& key,
               ConfigurationStore::ptr store)
    : ConfigurationVarBase(key, store),
      intValue_(0)
{}

int64_t IntVar::get() const {
    return intValue_;
}

void IntVar::updateImpl() {
    try {
        intValue_ = lexical_cast<int64_t>(value_);
    } catch(boost::bad_lexical_cast&) {
        MORDOR_LOG_ERROR(g_log) << this << " cannot parse from '" << value_ <<
            "'";
        throw;
    }
}

}  // namespace lightning
