#include "value.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/assert.h>
#include <string.h>

namespace lightning {
namespace paxos {

using std::string;
using boost::shared_ptr;

const uint32_t Value::kMaxValueSize;

Value::Value()
{}

Value::Value(const Guid& valueId,
             shared_ptr<string> data)
    : valueId_(valueId),
      data_(data)
{
    MORDOR_ASSERT(!!data);
    MORDOR_ASSERT(data->length() <= kMaxValueSize);
}

void Value::set(const Guid& valueId,
                shared_ptr<string> data)
{
    MORDOR_ASSERT(!!data);
    MORDOR_ASSERT(data->length() <= kMaxValueSize);
    valueId_ = valueId;
    data_ = data;
}

void Value::release(Guid* valueId,
                    shared_ptr<string>* data)
{
    MORDOR_ASSERT(!!data_);
    *valueId = valueId_;
    valueId_ = Guid();
    *data = data_;
    data_.reset();
}

void Value::reset() {
    valueId_ = Guid();
    data_.reset();
}

const Guid& Value::valueId() const {
    return valueId_;
}

size_t Value::size() const {
    MORDOR_ASSERT(!!data_);
    return data_->length();
}

Value Value::parse(const ValueData& valueData) {
    Guid valueId = Guid::parse(valueData.id());
    // TODO(skywalker): release_data with newer protobuf.
    shared_ptr<string> data(new string(valueData.data()));
    MORDOR_ASSERT(!!data && data->length() <= kMaxValueSize);
    return Value(valueId, data);
}

void Value::serialize(ValueData* data) const {
    MORDOR_ASSERT(!!data_);
    valueId_.serialize(data->mutable_id());
    data->set_data(*data_);
}

std::ostream& Value::output(std::ostream& os) const {
    if(!data_) {
        os << "(null value)";
    } else {
        os << "Value(" << valueId_ << ", size=" << data_->length() << ")";
    }
    return os;
}

}  // namespace paxos
}  // namespace lightning
