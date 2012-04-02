#include "sleep_helper.h"
#include <mordor/assert.h>
#include <mordor/sleep.h>
#include <mordor/timer.h>
#include <algorithm>

namespace lightning {

using Mordor::IOManager;
using Mordor::TimerManager;
using Mordor::sleep;
using std::max;

const int64_t SleepHelper::kEpollSleepPrecision;

SleepHelper::SleepHelper(IOManager* ioManager,
                         int64_t sleepInterval,
                         int64_t sleepPrecision)
    : ioManager_(ioManager),
      sleepInterval_(sleepInterval),
      sleepPrecision_(sleepPrecision)
{}

void SleepHelper::startWaiting() {
    MORDOR_ASSERT(waitStartTime_ == 0);
    waitStartTime_ = TimerManager::now();
}

void SleepHelper::stopWaiting() {
    MORDOR_ASSERT(waitStartTime_ != 0);
    int64_t waitTime = TimerManager::now() - waitStartTime_;
    waitStartTime_ = 0;
    accumulatedSleepTime_ = max(int64_t(0), accumulatedSleepTime_ - waitTime);
}

void SleepHelper::wait() {
    accumulatedSleepTime_ += sleepInterval_;
    if(accumulatedSleepTime_ > sleepPrecision_) {
        startWaiting();
        sleep(*ioManager_, accumulatedSleepTime_);
        stopWaiting();
    }
}

}  // namespace lightning
