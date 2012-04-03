#include "value.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/assert.h>
#include <string.h>

namespace lightning {
namespace paxos {

Value::ptr Value::parse(const ValueData& valueData) {
    Value::ptr value(new Value);
    value->valueId = Guid::parse(valueData.id());
    value->size = valueData.data().length();
    MORDOR_ASSERT(valueData.data().length() <= Value::kMaxValueSize);
    memcpy(value->data, valueData.data().c_str(), valueData.data().length());
    return value;
}

}  // namespace paxos
}  // namespace lightning
