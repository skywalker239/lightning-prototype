#include "tcp_value_receiver.h"
#include "proto/rpc_messages.pb.h"
#include "value_buffer.h"
#include <mordor/exception.h>
#include <mordor/log.h>
#include <vector>

namespace lightning {

using Mordor::Exception;
using Mordor::IOManager;
using Mordor::Log;
using Mordor::Logger;
using Mordor::Socket;
using paxos::Value;
using std::vector;

static Logger::ptr g_log = Log::lookup("lightning:tcp_value_receiver");

TcpValueReceiver::TcpValueReceiver(
    size_t valueBufferSize,
    ProposerState::ptr proposer,
    BlockingQueue<Value>::ptr submitQueue,
    IOManager* ioManager,
    Socket::ptr listenSocket)
    : valueBufferSize_(valueBufferSize),
      proposer_(proposer),
      submitQueue_(submitQueue),
      ioManager_(ioManager),
      listenSocket_(listenSocket)
{}

void TcpValueReceiver::run() {
    while(true) {
        Socket::ptr connectionSocket;
        try {
            connectionSocket = listenSocket_->accept();
        } catch(Exception& e) {
            MORDOR_LOG_ERROR(g_log) << this << " exception on accept: " <<
                e.what() << " || " <<
                boost::current_exception_diagnostic_information();
        }

        MORDOR_LOG_DEBUG(g_log) << this << " new value stream from " <<
            *(connectionSocket->remoteAddress());

        ioManager_->schedule(
            boost::bind(
                &TcpValueReceiver::handleValueStream,
                shared_from_this(),
                connectionSocket));
    }
}

void TcpValueReceiver::handleValueStream(Socket::ptr socket) {
    MORDOR_LOG_DEBUG(g_log) << this << " handling value stream from " <<
        *socket->remoteAddress();
    ValueBuffer valueBuffer(valueBufferSize_,
                            proposer_,
                            submitQueue_);
    while(true) {
        Value value;
        try {
            if(!readValue(socket, &value)) {
                MORDOR_LOG_WARNING(g_log) << this <<
                    " cannot read request, closing connection to " <<
                    *(socket->remoteAddress());
                return;
            }
        } catch(Exception& e) {
            MORDOR_LOG_ERROR(g_log) << this << " socket exception on " <<
                *socket->remoteAddress() << ": " << e.what();
            return;
        }
        MORDOR_LOG_TRACE(g_log) << this << " read Value(" <<
            value.valueId() << ", " << value.size() << ") from " <<
            *(socket->remoteAddress());
        valueBuffer.pushValue(value);
    }
}

bool TcpValueReceiver::readValue(Socket::ptr socket, Value* value) {
    FixedSizeHeaderData header;
    header.set_size(0);
    char messageHeaderData[header.ByteSize()];
    readFromSocket(socket, header.ByteSize(), messageHeaderData);
    if(!header.ParseFromArray(messageHeaderData, header.ByteSize())) {
        MORDOR_LOG_WARNING(g_log) << this << " cannot parse request from [" <<
                                     *(socket->remoteAddress()) << "]";
        return false;
    }
    vector<char> rawValueData(header.size(), 0);
    readFromSocket(socket, header.size(), &rawValueData[0]);
    ValueData valueData;
    if(!valueData.ParseFromArray(&rawValueData[0], header.size())) {
        MORDOR_LOG_WARNING(g_log) << this << " cannot parse value from [" <<
            *(socket->remoteAddress()) << "]";
        return false;
    }
    *value = Value::parse(valueData);
    return true;
}

void TcpValueReceiver::readFromSocket(Socket::ptr socket,
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

}  // namespace lightning
