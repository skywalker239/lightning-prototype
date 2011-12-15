#include "batch_phase1_request.h"
#include "proto/batch_phase1.pb.h"
#include <mordor/assert.h>
#include <mordor/log.h>
#include <algorithm>

namespace lightning {

using Mordor::Address;
using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using paxos::BallotId;
using paxos::InstanceId;
using std::make_pair;
using std::map;
using std::min;
using std::max;
using std::vector;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:batch_phase1_request");

BatchPhase1Request::BatchPhase1Request(
    RingConfiguration::const_ptr ringConfiguration,
    BallotId ballotId,
    InstanceId instanceRangeBegin,
    InstanceId instanceRangeEnd)
    : ringConfiguration_(ringConfiguration),
      ballotId_(ballotId),
      instanceRangeBegin_(instanceRangeBegin),
      instanceRangeEnd_(instanceRangeEnd),
      notAcked_(ringConfiguration_->ringAddresses().begin(),
                ringConfiguration_->ringAddresses().end()),
      status_(IN_PROGRESS),
      result_(PENDING),
      retryStartInstanceId_(0),
      event_(true)
{}

BatchPhase1Request::Result BatchPhase1Request::result() const {
    FiberMutex::ScopedLock lk(mutex_);
    return result_;
}

const map<InstanceId, BallotId>& BatchPhase1Request::reservedInstances() const
{
    FiberMutex::ScopedLock lk(mutex_);
    return reservedInstances_;
}

InstanceId BatchPhase1Request::retryStartInstanceId() const {
    FiberMutex::ScopedLock lk(mutex_);
    return retryStartInstanceId_;
}

const string BatchPhase1Request::requestString() const {
    FiberMutex::ScopedLock lk(mutex_);

    MORDOR_LOG_TRACE(g_log) << this << " ringId=" <<
                               ringConfiguration_->ringId() <<
                               " ballotId=" <<
                               ballotId_ << " instances=[" <<
                               instanceRangeBegin_ << ", " <<
                               instanceRangeEnd_ << ")";
    BatchPhase1RequestData data;
    data.set_ring_id(ringConfiguration_->ringId());
    data.set_ballot_id(ballotId_);
    data.set_instance_range_start(instanceRangeBegin_);
    data.set_instance_range_end(instanceRangeEnd_);

    string s;
    data.SerializeToString(&s);
    return s;
}

void BatchPhase1Request::onReply(Address::ptr source,
                                 const string& reply)
{
    FiberMutex::ScopedLock lk(mutex_);

    BatchPhase1ResponseData data;
    if(!data.ParseFromString(reply)) {
        MORDOR_LOG_WARNING(g_log) << this << " malformed reply from " <<
                                     *source;
        return;
    }
    
    switch(data.status()) {
        case BatchPhase1ResponseData::OK:
            MORDOR_LOG_TRACE(g_log) << this << " OK from " << *source;
            break;
        case BatchPhase1ResponseData::START_IID_TOO_LOW:
            result_ = START_IID_TOO_LOW;
            MORDOR_ASSERT(instanceRangeEnd_ <=
                          data.retry_start_instance_id());

            retryStartInstanceId_ = min(retryStartInstanceId_,
                                        data.retry_start_instance_id());
            MORDOR_LOG_TRACE(g_log) << this << " new start iid=" <<
                                       retryStartInstanceId_ << " from " <<
                                       *source;
            break;
        case BatchPhase1ResponseData::NOT_ALL_OPEN:
            result_ = (result_ == PENDING) ? NOT_ALL_OPEN : result_;
            MORDOR_LOG_TRACE(g_log) << this << " NOT_ALL_OPEN from " <<
                                       *source;
            result_ = (result_ == PENDING) ? NOT_ALL_OPEN : result_;
            MORDOR_ASSERT(data.reserved_instances_size() ==
                          data.reserved_instance_ballots_size());
            for(int i = 0; i < data.reserved_instances_size(); ++i) {
                InstanceId instanceId = data.reserved_instances(i);
                BallotId   ballotId   = data.reserved_instance_ballots(i);
                auto reservedInstanceIter =
                    reservedInstances_.find(instanceId);
                if(reservedInstanceIter != reservedInstances_.end()) {
                    reservedInstanceIter->second =
                        max(reservedInstanceIter->second, ballotId);
                    MORDOR_LOG_TRACE(g_log) << this << " iid=" << instanceId <<
                                               " reserved with ballot=" <<
                                               reservedInstanceIter->second;
                } else {
                    reservedInstances_.insert(make_pair(instanceId, ballotId));
                    MORDOR_LOG_TRACE(g_log) << this << " iid=" << instanceId <<
                                               " reserved with ballot=" <<
                                               ballotId;
                }
            }
            break;
    }
    notAcked_.erase(source);
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << " got all replies";
        result_ = (result_ == PENDING) ? ALL_OPEN : result_;
        status_ = OK;
        event_.set();
    }
}

void BatchPhase1Request::onTimeout() {
    FiberMutex::ScopedLock lk(mutex_);
    if(notAcked_.empty()) {
        MORDOR_LOG_TRACE(g_log) << this << " onTimeout() with no pending acks";
        status_ = OK;
    } else {
        MORDOR_LOG_TRACE(g_log) << this << " timed out";
        status_ = TIMED_OUT;
    }
    event_.set();
}

void BatchPhase1Request::wait() {
    event_.wait();
    FiberMutex::ScopedLock lk(mutex_);
}

SyncGroupRequest::Status BatchPhase1Request::status() const {
    FiberMutex::ScopedLock lk(mutex_);
    return status_;
}

}  // namespace lightning
