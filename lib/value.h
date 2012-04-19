#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include <stdint.h>
#include <boost/shared_ptr.hpp>
#include <iostream>
#include <string>

namespace lightning {

class ValueData;

namespace paxos {

//! A string of bytes together with a GUID.
//  Not fiber-safe.
class Value {
public:
    //! Empty value with zero id and no data.
    Value();
    //! Value with given id and data.
    Value(const Guid& valueId,
          boost::shared_ptr<std::string> data);

    //! Overwrites the previous id and data.
    void set(const Guid& valueId,
             boost::shared_ptr<std::string> data);

    //! Extracts id and data from the value, leaving it empty.
    //  Asserts on empty data.
    void release(Guid* valueId,
                 boost::shared_ptr<std::string>* data);

    //! Release data, reset guid to zero.
    void reset();

    //! Current value id.
    const Guid& valueId() const;

    //! Current value size. Asserts on empty data.
    size_t size() const;

    //! Serialize to protobuf.
    void serialize(ValueData* data) const;

    //! Parse from protobuf.
    static Value parse(const ValueData& data);

    //! For debug output
    std::ostream& output(std::ostream& os) const;

    static const uint32_t kMaxValueSize = 8000;
private:
    Guid valueId_;
    boost::shared_ptr<std::string> data_;
};

inline
std::ostream& operator<<(std::ostream& os, const Value& v) {
    return v.output(os);
}

}  // namespace paxos
}  // namespace lightning
