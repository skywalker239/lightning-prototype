#include "tcp_recovery_service.h"
#include "paxos_defs.h"
#include "value.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/log.h>
#include <mordor/statistics.h>

namespace lightning {

using Mordor::Address;
using Mordor::CountStatistic;
using Mordor::Exception;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using Mordor::Statistics;
using Mordor::TimerManager;
using paxos::BallotId;
using paxos::InstanceId;
using paxos::kInvalidBallotId;
using paxos::Value;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:tcp_recovery_service");

TcpRecoveryService::TcpRecoveryService(IOManager* ioManager,
                                       Socket::ptr listenSocket,
                                       ValueCache::ptr valueCache)
    : ioManager_(ioManager),
      listenSocket_(listenSocket),
      valueCache_(valueCache)
{}

void TcpRecoveryService::run() {
    while(true) {
        Socket::ptr connectionSocket;
        try {
            connectionSocket = listenSocket_->accept();
        } catch(Exception& e) {
            MORDOR_LOG_ERROR(g_log) << this << " exception on accept: " <<
                                       boost::current_exception_diagnostic_information();
        }

        MORDOR_LOG_DEBUG(g_log) << this << " new recovery connection from " <<
                                   *(connectionSocket->remoteAddress());
        ioManager_->schedule(
            boost::bind(
                &TcpRecoveryService::handleRecoveryConnection,
                shared_from_this(),
                connectionSocket));
    }
}

void TcpRecoveryService::handleRecoveryConnection(Socket::ptr socket) {
    while(true) {
        try {
            BatchRecoveryRequestData request;
            if(!readRequest(socket, &request)) {
                MORDOR_LOG_WARNING(g_log) << this << " cannot read request," <<
                                             " closing [" <<
                                             *(socket->remoteAddress()) <<
                                             "]";
                return;
            }
            BatchRecoveryReplyData reply;
            handleRequest(socket, request, &reply);
            sendReply(socket, reply);
        } catch(Exception&) {
            MORDOR_LOG_INFO(g_log) << this << " connection from [" <<
                                      *(socket->remoteAddress()) <<
                                      "] went down";
            return;
        }
    }
}

bool TcpRecoveryService::readRequest(Socket::ptr socket,
                                     BatchRecoveryRequestData* request)
{
    uint64_t startTime = TimerManager::now();
    FixedSizeHeaderData header;
    header.set_size(0);
    char messageHeaderData[header.ByteSize()];
    readFromSocket(socket, header.ByteSize(), messageHeaderData);
    if(!header.ParseFromArray(messageHeaderData, header.ByteSize())) {
        MORDOR_LOG_WARNING(g_log) << this << " cannot parse request from [" <<
                                     *(socket->remoteAddress()) << "]";
        return false;
    }
    vector<char> requestData(header.size(), 0);
    readFromSocket(socket, header.size(), &requestData[0]);
    if(!request->ParseFromArray(&requestData[0], header.size())) {
        MORDOR_LOG_WARNING(g_log) << this <<  "cannot parse request from [" <<
                                     *(socket->remoteAddress()) << "]";
        return false;
    }
    MORDOR_LOG_DEBUG(g_log) << this << " read request: " <<
        (TimerManager::now() - startTime) << " us, " <<
        header.size() << " bytes";
    return true;
}

void TcpRecoveryService::sendReply(Socket::ptr socket,
                                   const BatchRecoveryReplyData& reply)
{
    uint64_t startTime = TimerManager::now();
    FixedSizeHeaderData header;
    header.set_size(reply.ByteSize());
    const size_t messageSize = header.ByteSize() + reply.ByteSize();
    vector<char> messageData(messageSize, 0);
    header.SerializeToArray(&messageData[0], header.ByteSize());
    reply.SerializeToArray(&messageData[header.ByteSize()], reply.ByteSize());

    size_t sent = 0;
    while(sent < messageSize) {
        sent += socket->send(&messageData[sent], messageSize - sent);
    }
    MORDOR_LOG_DEBUG(g_log) << this << " write reply: " <<
        (TimerManager::now() - startTime) << " us, " <<
        messageSize << " bytes";
}

void TcpRecoveryService::readFromSocket(Socket::ptr socket,
                                        size_t bytes,
                                        char* destination)
{
    size_t bytesRead = 0;
    while(bytesRead < bytes) {
        size_t currentRead =
            socket->receive(destination + bytesRead, bytes - bytesRead);
        if(currentRead == 0) {
            MORDOR_LOG_DEBUG(g_log) << this << " connection to " <<
                *(socket->remoteAddress()) << " went down";
            MORDOR_THROW_EXCEPTION_FROM_LAST_ERROR_API("recv");
        }
        bytesRead += currentRead;
    }
}

void TcpRecoveryService::handleRequest(Socket::ptr socket,
                                       const BatchRecoveryRequestData& request,
                                       BatchRecoveryReplyData* reply)
{
    uint64_t startTime = TimerManager::now();
    const Guid& requestEpoch = Guid::parse(request.epoch());
    MORDOR_LOG_DEBUG(g_log) << this << " recover " <<
        request.instances_size() << " instances for epoch " <<
        requestEpoch << " from " << *(socket->remoteAddress());
    for(int i = 0; i < request.instances_size(); ++i) {
        InstanceId instanceId = request.instances(i);
        MORDOR_LOG_TRACE(g_log) << this << " recover(" << requestEpoch <<
            ", " << instanceId << " from " << *(socket->remoteAddress());


        Value value;
        auto result = valueCache_->query(requestEpoch, instanceId, &value);
        switch(result) {
            case ValueCache::TOO_OLD: case ValueCache::WRONG_EPOCH: // XXX
                MORDOR_LOG_TRACE(g_log) << this << " iid " << instanceId <<
                    " forgotten";
                reply->add_forgotten_instances(instanceId);
                break;
            case ValueCache::NOT_YET:
                MORDOR_LOG_TRACE(g_log) << this << " iid " << instanceId <<
                    " not committed";
                reply->add_not_committed_instances(instanceId);
                break;
            case ValueCache::OK:
            {
                MORDOR_LOG_TRACE(g_log) << this << " iid " << instanceId <<
                    " -> " << value;
                InstanceData* instanceData = reply->add_recovered_instances();
                instanceData->set_instance_id(instanceId);
                value.serialize(instanceData->mutable_value());
                break;
            }
            default:
                MORDOR_ASSERT(1 == 0);
        }
    }
    requestEpoch.serialize(reply->mutable_epoch());
    MORDOR_LOG_DEBUG(g_log) << this << " handle request: " <<
        (TimerManager::now() - startTime) << " us";
}

}  // namespace lightning
