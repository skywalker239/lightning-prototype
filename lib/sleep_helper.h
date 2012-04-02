#pragma once

#include <mordor/iomanager.h>

namespace lightning {

//! Sometimes we need to perform some action (i.e. send a packet)
//  repeatedly within some fixed rate. At high rates we are hampered
//  by the limited precision of epoll (1ms) and don't yet care enough
//  to utilize something more precise like timerfd.
//  This class approximates low-latency sleep by accumulating unused sleep
//  time and not sleeping if the accumulated sleep time is lower than the
//  sleep precision.
class SleepHelper {
public:
    SleepHelper(Mordor::IOManager* ioManager,
                int64_t sleepInterval,
                int64_t sleepPrecision);

    //! Accounts for waiting for external events, decreases the sleeping time.
    void startWaiting();
    void stopWaiting();

    //! Waits for sleepInterval us using the approximation algorithm above.
    void wait();

    //! sleep precision of epoll_wait(2).
    static const int64_t kEpollSleepPrecision = 1000;
private:
    Mordor::IOManager* ioManager_;

    const int64_t sleepInterval_;
    const int64_t sleepPrecision_;
    int64_t accumulatedSleepTime_;

    int64_t waitStartTime_;
};

}  // namespace lightning
