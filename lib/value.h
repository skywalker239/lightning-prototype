#pragma once

#include "guid.h"
#include "paxos_defs.h"
#include <stdint.h>

namespace lightning {
namespace paxos {

//! For now, just a fixed-size buffer with
//  a Guid.
struct Value {
    static const uint32_t kMaxValueSize = 8000;
    typedef boost::shared_ptr<Value> ptr;

    Guid valueId;
    uint32_t size;
    char data[kMaxValueSize];
} __attribute__((packed));

}  // namespace paxos
}  // namespace lightning
