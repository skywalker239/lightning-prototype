#include "recovery_connection.h"
#include "recovery_manager.h"
#include "sleep_helper.h"
#include "value.h"
#include "proto/rpc_messages.pb.h"
#include <stdexcept>
#include <mordor/log.h>
#include <mordor/sleep.h>
#include <mordor/statistics.h>

namespace lightning {

using paxos::InstanceId;
using paxos::BallotId;
using paxos::Value;
using Mordor::Address;
using Mordor::CountStatistic;
using Mordor::FiberMutex;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using Mordor::Statistics;
using std::logic_error;
using std::string;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:recovery_connection");

const size_t RecoveryConnection::kMaxBatchSize;

RecoveryConnection::RecoveryConnection(
    const string& name,
    uint32_t metric,
    uint64_t queuePollIntervalUs,
    uint64_t connectionRetryIntervalUs,
    uint64_t socketTimeoutUs,
    uint64_t instanceRetryIntervalUs,
    Address::ptr address,
    RecoveryManager::ptr recoveryManager,
    IOManager* ioManager)
    : name_(name),
      metric_(metric),
      queuePollIntervalUs_(queuePollIntervalUs),
      connectionRetryIntervalUs_(connectionRetryIntervalUs),
      socketTimeoutUs_(socketTimeoutUs),
      instanceRetryIntervalUs_(instanceRetryIntervalUs),
      address_(address),
      recoveryManager_(recoveryManager),
      ioManager_(ioManager),
      connected_(false),
      queueSize_(Statistics::registerStatistic(name_ + ".queue_size",
                                               CountStatistic<uint64_t>())),
      recoveryFailures_(
        Statistics::registerStatistic(name_ + ".recovery_failures",
                                      CountStatistic<uint64_t>())),
      recoveryRetries_(
        Statistics::registerStatistic(name_ + ".recovery_retries",
                                      CountStatistic<uint64_t>())),
      recoveredInstances_(
        Statistics::registerStatistic(name_ + ".recovered_instances",
                                      CountStatistic<uint64_t>()))
{}

void RecoveryConnection::run() {
    while(true) {
        openConnection();
        processQueue();
    }
}

bool RecoveryConnection::addInstance(RecoveryRecord::ptr record) {
    FiberMutex::ScopedLock lk(mutex_);
    if(!connected_) {
        return false;
    }
    recoveryQueue_.push(record);
    queueSize_.increment();
    return true;
}

void RecoveryConnection::openConnection() {
    {
        FiberMutex::ScopedLock lk(mutex_);
        MORDOR_ASSERT(connected_ == false);
    }

    while(true) {
        MORDOR_LOG_INFO(g_log) << this << "[" << name_ << "]: connecting...";
        socket_ = address_->createSocket(*ioManager_, SOCK_STREAM);
        socket_->sendTimeout(socketTimeoutUs_);
        socket_->receiveTimeout(socketTimeoutUs_);
        try {
            socket_->connect(address_);
            FiberMutex::ScopedLock lk(mutex_);
            connected_ = true;
            recoveryManager_->enableConnection(shared_from_this());
            MORDOR_LOG_INFO(g_log) << this << "[" << name_ << "]: connected.";
            return;
        } catch(...) {
            MORDOR_LOG_INFO(g_log) << this << "[" << name_ << "]: " <<
                                      "Could not connect: " <<
                                      boost::current_exception_diagnostic_information();
        }
        sleep(*ioManager_, connectionRetryIntervalUs_);
    }
}

void RecoveryConnection::processQueue() {
    SleepHelper sleeper(ioManager_,
                        queuePollIntervalUs_,
                        SleepHelper::kEpollSleepPrecision);
    while(true) {
        sleeper.wait();
        sleeper.startWaiting();
        {
            FiberMutex::ScopedLock lk(mutex_);
            MORDOR_ASSERT(connected_ == true);
        }

        MORDOR_LOG_DEBUG(g_log) << this << "[" << name_ << "]: " <<
                                   "checking the queue";

        Guid batchEpoch;
        vector<RecoveryRecord::ptr> currentBatch;
        if(!getCurrentBatch(&batchEpoch, &currentBatch)) {
            sleeper.stopWaiting();
            continue;
        }

        MORDOR_LOG_DEBUG(g_log) << this << "[" << name_ << "]: " <<
            "recovering " << currentBatch.size() << " instances for epoch " <<
            batchEpoch;

        BatchRecoveryReplyData replyData;
        try {
            sendRequest(batchEpoch, currentBatch);
            readReply(&replyData);
        } catch(...) {
            MORDOR_LOG_INFO(g_log) << this << "[" << name_ << "]: " <<
                " connection failed: " <<
                boost::current_exception_diagnostic_information();

            {
                FiberMutex::ScopedLock lk(mutex_);
                connected_ = false;
            }
            recoveryManager_->disableConnection(shared_from_this());
            handoffInstances(currentBatch);
            return;
        }
        processReply(replyData);
        sleeper.stopWaiting();
    }
}

bool RecoveryConnection::getCurrentBatch(
    Guid* batchEpoch,
    vector<RecoveryRecord::ptr>* currentBatch)
{
    FiberMutex::ScopedLock lk(mutex_);
    if(recoveryQueue_.empty()) {
        MORDOR_LOG_DEBUG(g_log) << this << "[" << name_ << "]: " <<
                                   "queue is empty";
        return false;
    }
    *batchEpoch = recoveryQueue_.front()->epoch();
    while(!recoveryQueue_.empty() &&
          (recoveryQueue_.front()->epoch() == *batchEpoch) &&
          currentBatch->size() < kMaxBatchSize)
    {
        MORDOR_LOG_TRACE(g_log) << this << "[" << name_ << "]: " <<
            " adding (" << *batchEpoch << ", " <<
            recoveryQueue_.front()->instanceId() << ")";
        currentBatch->push_back(recoveryQueue_.front());
        recoveryQueue_.pop();
        queueSize_.decrement();
    }
    return true;
}

void RecoveryConnection::sendRequest(
    const Guid& batchEpoch,
    const vector<RecoveryRecord::ptr>& batch)
{
    BatchRecoveryRequestData requestData;
    batchEpoch.serialize(requestData.mutable_epoch());
    for(size_t i = 0; i < batch.size(); ++i) {
        requestData.add_instances(batch[i]->instanceId());
    }
    FixedSizeHeaderData header;
    header.set_size(requestData.ByteSize());
    vector<char> data(header.ByteSize() + requestData.ByteSize(), 0);
    header.SerializeToArray(&data[0], header.ByteSize());
    requestData.SerializeToArray(&data[header.ByteSize()], requestData.ByteSize());
    doSend(&data[0], data.size());
}

void RecoveryConnection::readReply(BatchRecoveryReplyData* replyData) {
    FixedSizeHeaderData header;
    header.set_size(0);
    char headerData[header.ByteSize()];
    doReceive(&headerData[0], header.ByteSize());
    if(!header.ParseFromArray(headerData, header.ByteSize())) {
        throw logic_error("Cannot parse reply header");
    }
    vector<char> replyBody(header.size(), 0);
    doReceive(&replyBody[0], header.size());
    if(!replyData->ParseFromArray(&replyBody[0], header.size())) {
        throw logic_error("Cannot parse reply body");
    }
}

void RecoveryConnection::processReply(
    const BatchRecoveryReplyData& replyData)
{
    Guid epoch = Guid::parse(replyData.epoch());
    for(int i = 0; i < replyData.recovered_instances_size(); ++i) {
        const InstanceData& instanceData = replyData.recovered_instances(i);
        InstanceId instanceId = instanceData.instance_id();
        Value value = Value::parse(instanceData.value());
        MORDOR_LOG_TRACE(g_log) << this << " recovered (" << epoch << ", " <<
            instanceId << ", " << value << ")";
        recoveryManager_->addRecoveredValue(epoch,
                                            instanceId,
                                            value);
        recoveredInstances_.increment();
    }
    for(int i = 0; i < replyData.not_committed_instances_size(); ++i) {
        InstanceId instanceId = replyData.not_committed_instances(i);
        MORDOR_LOG_TRACE(g_log) << this << " (" << epoch << ", " <<
            ") not committed, scheduling retry";
        ioManager_->registerTimer(instanceRetryIntervalUs_,
                                  boost::bind(&RecoveryConnection::retryInstance,
                                              shared_from_this(),
                                              RecoveryRecord::ptr(
                                                new RecoveryRecord(
                                                    epoch,
                                                    instanceId))));
        recoveryRetries_.increment();
    }
    for(int i = 0; i < replyData.forgotten_instances_size(); ++i) {
        InstanceId instanceId = replyData.forgotten_instances(i);
        MORDOR_LOG_WARNING(g_log) << this << " (" << epoch << ", " <<
            instanceId << ") forgotten!";
        recoveryFailures_.increment();
    }
}

void RecoveryConnection::handoffInstances(
    const vector<RecoveryRecord::ptr>& lastBatch)
{
    for(size_t i = 0; i < lastBatch.size(); ++i) {
        handoffInstance(lastBatch[i]);
    }
    while(true) {
        RecoveryRecord::ptr record;
        {
            FiberMutex::ScopedLock lk(mutex_);
            if(recoveryQueue_.empty()) {
                break;
            }
            record = recoveryQueue_.front();
            recoveryQueue_.pop();
        }
        handoffInstance(record);
    }
}

void RecoveryConnection::handoffInstance(const RecoveryRecord::ptr& record) {
    const Guid& epoch = record->epoch();
    const InstanceId& instanceId = record->instanceId();
    MORDOR_LOG_TRACE(g_log) << this << " returning (" << epoch << ", " <<
        instanceId << ") to recovery manager";
        recoveryManager_->addInstance(epoch,
                                      instanceId);
}

void RecoveryConnection::retryInstance(RecoveryRecord::ptr record) {
    const Guid& epoch = record->epoch();
    const InstanceId& instanceId = record->instanceId();
    MORDOR_LOG_TRACE(g_log) << this << " returning (" << epoch << ", " <<
        instanceId << ") to recovery manager with random retry";
        recoveryManager_->addInstance(epoch,
                                      instanceId, false);
//    if(!addInstance(record)) {
//        handoffInstance(record);
//    }
}

void RecoveryConnection::doSend(const char* data, uint64_t length) {
    uint64_t sent = 0;
    while(sent < length) {
        sent += socket_->send(data + sent, length - sent);
    }
}

void RecoveryConnection::doReceive(char* data, uint64_t length) {
    uint64_t received = 0;
    while(received < length) {
        uint64_t currentReceived =
            socket_->receive(data + received, length - received);
        if(currentReceived == 0) {
            MORDOR_LOG_DEBUG(g_log) << this << " connection to " <<
                *(socket_->remoteAddress()) << " went down.";
            MORDOR_THROW_EXCEPTION_FROM_LAST_ERROR_API("recv");
        }
        received += currentReceived;
    }
}

}  // namespace lightning
