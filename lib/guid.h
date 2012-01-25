#pragma once

#include <mordor/atomic.h>
#include <boost/shared_ptr.hpp>
#include <iostream>
#include <string>
#include <stdint.h>
#include <sys/types.h>

namespace lightning {

//! A 128-bit globally unique identifier.
class Guid {
public:
    bool operator==(const Guid& rhs) const;

    bool operator!=(const Guid& rhs) const;

    bool operator<(const Guid& rhs) const;

    void serialize(std::string* destination) const;

    static Guid parse(const std::string& serialized);

    //! Constructs a special GUID which is really a 128-bit MurmurHash3
    //  of passed data.
    static Guid fromData(const void* data, size_t len);

    std::ostream& print(std::ostream& os) const;
    
    Guid();
private:

    uint32_t parts_[4];

    static const uint32_t kHashSeed = 239;

    friend class GuidGenerator;
};

std::ostream& operator<<(std::ostream& os, const Guid& guid);

//! Generates Guids such that the first two 32-bit parts
//  are a secure hash of pseudo-unique data, the third part is a 32-bit
//  timestamp and the fourth is the current value of an incrementing
//  atomic counter specific to this GuidGenerator.
class GuidGenerator {
public:
    // The initial pseudo-unique state consists of the current
    // high-resolution timestamp, the current monotonic clock value
    // and the tid of calling thread.
    GuidGenerator();

    typedef boost::shared_ptr<GuidGenerator> ptr;

    void init(uint64_t currentTime,
              uint64_t currentUptime,
              pid_t    tid);


    Guid generate();
private:
    struct Seed {
        uint64_t startTime;
        uint64_t startUptime;
        pid_t    tid;
    } __attribute((packed))__;

    static void getTime(uint64_t* time, uint64_t* uptime);

    Seed seed_;
    Mordor::Atomic<uint32_t> counter_;

    static const uint32_t kHashSeed = 239;
};

}  // namespace lightning
