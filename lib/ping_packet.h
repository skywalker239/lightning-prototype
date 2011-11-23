#pragma once

#include <stdint.h>

namespace lightning {

struct PingPacket {
    uint64_t id;
    uint64_t senderNow;

    PingPacket(uint64_t _id, uint64_t _senderNow)
        : id(_id),
          senderNow(_senderNow)
    {}
} __attribute__((packed));

}  // namespace lightning
