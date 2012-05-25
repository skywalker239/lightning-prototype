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
using paxos::BallotId;
using paxos::InstanceId;
using paxos::kInvalidBallotId;
using paxos::Value;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:tcp_recovery_service");

TcpRecoveryService::TcpRecoveryService(IOManager* ioManager,
                                       Socket::ptr listenSocket,
                                       AcceptorState::ptr acceptorState)
    : ioManager_(ioManager),
      listenSocket_(listenSocket),
      acceptorState_(acceptorState)
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

        MORDOR_LOG_TRACE(g_log) << this << " new recovery connection from " <<
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
        } catch(Exception& e) {
            MORDOR_LOG_ERROR(g_log) << this << " socket exception on [" <<
                                       *(socket->remoteAddress()) << "]";
            return;
        }
    }
}

bool TcpRecoveryService::readRequest(Socket::ptr socket,
                                     BatchRecoveryRequestData* request)
{
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
    return true;
}

void TcpRecoveryService::sendReply(Socket::ptr socket,
                                   const BatchRecoveryReplyData& reply)
{
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
            MORDOR_LOG_TRACE(g_log) << this << " connection to " <<
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
    const Guid& requestEpoch = Guid::parse(request.epoch());
    for(int i = 0; i < request.instances_size(); ++i) {
        InstanceId instanceId = request.instances(i);
        MORDOR_LOG_TRACE(g_log) << this << " recover(" << requestEpoch <<
            ", " << instanceId << " from " << *(socket->remoteAddress());


        Value value;
        BallotId ballotId = kInvalidBallotId;
        if(!acceptorState_->getCommittedInstance(requestEpoch,
                                                 instanceId,
                                                 &value,
                                                 &ballotId))
        {
            if(instanceId < acceptorState_->firstNotForgottenInstance()) {
                MORDOR_LOG_TRACE(g_log) << this << " recover: iid " <<
                                           instanceId << " forgotten";
                reply->add_forgotten_instances(instanceId);
            } else {
                MORDOR_LOG_TRACE(g_log) << this << " recover: iid " <<
                                           instanceId << " not committed";
                reply->add_not_committed_instances(instanceId);
            }
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " recover : iid " <<
                                       instanceId << " -> (" << value <<
                                       ", " << ballotId << ")";
            InstanceData* instanceData = reply->add_recovered_instances();
            instanceData->set_instance_id(instanceId);
            instanceData->set_ballot(ballotId);
            value.serialize(instanceData->mutable_value());
        }
    }
    requestEpoch.serialize(reply->mutable_epoch());
}

}  // namespace lightning