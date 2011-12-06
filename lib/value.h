#pragma once

#include "paxos_defs.h"
#include <stdint.h>

namespace lightning {
namespace paxos {

//! For now, just a fixed-size buffer with
//  a ValueId attached.
struct Value {
    static const uint32_t kMaxValueSize = 8000;

    uint32_t size;
    char data[kMaxValueSize];
} __attribute__((packed));

}  // namespace paxos
}  // namespace lightning
