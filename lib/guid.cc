#include "guid.h"
#include "MurmurHash3.h"
#include <mordor/timer.h>
#include <syscall.h>
#include <sys/time.h>

namespace lightning {

using Mordor::Atomic;
using Mordor::TimerManager;
using std::ostream;
using std::string;

GuidGenerator::GuidGenerator()
    : counter_(0)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    uint64_t time, uptime;
    getTime(&time, &uptime);
    const pid_t tid = syscall(__NR_gettid);

    init(time, uptime, tid);
}

void GuidGenerator::getTime(uint64_t* time, uint64_t* uptime) {
    const uint32_t kMicrosecondsPerSecond = 1000000;
    struct timeval tv;
    gettimeofday(&tv, NULL);

    *time = tv.tv_sec * kMicrosecondsPerSecond +
                                 tv.tv_usec;
    *uptime = TimerManager::now();
}

void GuidGenerator::init(uint64_t currentTime,
                         uint64_t currentUptime,
                         pid_t    tid)
{
    seed_.startTime = currentTime;
    seed_.startUptime = currentUptime;
    seed_.tid = tid;
}

Guid GuidGenerator::generate() {
    // (time, uptime)
    const size_t kAdditionalSpace = 2 * sizeof(uint64_t);
    const size_t kMicrosecondsPerSecond = 1000000;

    char data[sizeof(Seed) + kAdditionalSpace];
    getTime((uint64_t*)data, (uint64_t*)(data + sizeof(uint64_t)));
    const uint32_t currentTime = *(uint64_t*)data / kMicrosecondsPerSecond;

    memcpy(data + kAdditionalSpace, &seed_, sizeof(Seed));
    
    uint32_t hash[4];
    MurmurHash3_x64_128(data, sizeof(data), kHashSeed, hash);
    //! Collapse the full 128-bit hash into 64 bits.
    hash[0] ^= hash[2];
    hash[1] ^= hash[3];
    hash[2] = currentTime;
    hash[3] = ++counter_;

    Guid guid;
    memcpy(guid.parts_, hash, sizeof(hash));
    return guid;
}

Guid::Guid() {
    parts_[0] = 0;
    parts_[1] = 0;
    parts_[2] = 0;
    parts_[3] = 0;
}

bool Guid::operator==(const Guid& rhs) const {
    return memcmp(parts_, rhs.parts_, sizeof(parts_)) == 0;
}

bool Guid::operator!=(const Guid& rhs) const {
    return !operator==(rhs);
}

bool Guid::operator<(const Guid& rhs) const {
    return memcmp(parts_, rhs.parts_, sizeof(parts_)) < 0;
}

void Guid::serialize(string* destination) const {
    const char* p = (const char*)parts_;
    destination->assign(p, p + sizeof(parts_));
}

Guid Guid::parse(const string& data) {
    Guid guid;
    memcpy(guid.parts_, data.c_str(), sizeof(parts_));
    return guid;
}

Guid Guid::fromData(const void* data, size_t len) {
    Guid guid;
    MurmurHash3_x64_128(data, len, kHashSeed, guid.parts_);
    return guid;
}

ostream& Guid::print(ostream& os) const {
    char buffer[8 * 4 + 3 + 1];
    sprintf(buffer, "%x-%x-%x-%x", parts_[0], parts_[1], parts_[2], parts_[3]);
    os << buffer;
    return os;
}

bool Guid::empty() const {
    return parts_[0] == 0 &&
           parts_[1] == 0 &&
           parts_[2] == 0 &&
           parts_[3] == 0;
}

ostream& operator<<(ostream& os, const Guid& guid) {
    return guid.print(os);
}

}  // namespace lightning
