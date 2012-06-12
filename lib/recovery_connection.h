#pragma once

#include "recovery_record.h"
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/socket.h>
#include <mordor/statistics.h>
#include <string>
#include <queue>

namespace lightning {

class RecoveryManager;
class BatchRecoveryReplyData;

class RecoveryConnection
    : public boost::enable_shared_from_this<RecoveryConnection>
{
public:
    typedef boost::shared_ptr<RecoveryConnection> ptr;

    RecoveryConnection(const std::string& name,
                       uint32_t metric,
                       uint64_t queuePollIntervalUs,
                       uint64_t connectionRetryIntervalUs,
                       uint64_t socketTimeoutUs,
                       uint64_t instanceRetryIntervalUs,
                       Mordor::Address::ptr address,
                       boost::shared_ptr<RecoveryManager> recoveryManager,
                       Mordor::IOManager* ioManager);

    void run();

    bool addInstance(RecoveryRecord::ptr recoveryRecord);

    const std::string& name() const { return name_; }

    uint32_t metric() const { return metric_; }
private:
    void retryInstance(RecoveryRecord::ptr recoveryRecord);

    void openConnection();

    void processQueue();

    bool getCurrentBatch(Guid* batchEpoch,
                         std::vector<RecoveryRecord::ptr>* currentBatch);

    void sendRequest(const Guid& batchEpoch,
                     const std::vector<RecoveryRecord::ptr>& batch);

    void readReply(BatchRecoveryReplyData* replyData);

    void processReply(const BatchRecoveryReplyData& replyData);

    void handoffInstances(const std::vector<RecoveryRecord::ptr>& lastBatch);

    void handoffInstance(const RecoveryRecord::ptr& record);

    void doSend(const char* data, uint64_t length);

    void doReceive(char* data, uint64_t length);

    const std::string& name_;
    const uint32_t metric_;
    const uint64_t queuePollIntervalUs_;
    const uint64_t connectionRetryIntervalUs_;
    const uint64_t socketTimeoutUs_;
    const uint64_t instanceRetryIntervalUs_;
    const Mordor::Address::ptr address_;
    boost::shared_ptr<RecoveryManager> recoveryManager_;
    Mordor::IOManager* ioManager_;

    std::queue<RecoveryRecord::ptr> recoveryQueue_;
    Mordor::Socket::ptr socket_;
    bool connected_;

    Mordor::CountStatistic<uint64_t>& queueSize_;
    Mordor::CountStatistic<uint64_t>& recoveryFailures_;
    Mordor::CountStatistic<uint64_t>& recoveryRetries_;
    Mordor::CountStatistic<uint64_t>& recoveredInstances_;

    mutable Mordor::FiberMutex mutex_;

    // about 50M, 60 is the protobuf limit for a single message
    static const size_t kMaxBatchSize = 6000;
};

}  // namespace lightning
