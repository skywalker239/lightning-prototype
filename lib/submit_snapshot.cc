#include "blocking_queue.h"
#include "guid.h"
#include "value.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/config.h>
#include <mordor/fibersynchronization.h>
#include <mordor/iomanager.h>
#include <mordor/log.h>
#include <mordor/socket.h>
#include <mordor/streams/std.h>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <set>

using namespace lightning;
using namespace paxos;
using namespace Mordor;
using namespace std;
using boost::shared_ptr;

static Logger::ptr g_log = Log::lookup("lightning:main");

class SubmitBuffer {
public:
    SubmitBuffer(size_t bufferSize,
                 BlockingQueue<Value>::ptr submitQueue)
        : bufferSize_(bufferSize),
          submitQueue_(submitQueue),
          canPush_(false)
    {
        canPush_.set();
    }

    void pushValue(const Value& value) {
        canPush_.wait();
        FiberMutex::ScopedLock lk(mutex_);
        pendingIds_.insert(value.valueId());
        if(pendingIds_.size() == bufferSize_) {
            MORDOR_LOG_INFO(g_log) << this << " buffer full";
            canPush_.reset();
        }
        MORDOR_LOG_TRACE(g_log) << this << " buffer push(" << value.valueId() << "), buffered=" << pendingIds_.size();
        submitQueue_->push(value);
    }

    void notify(const Guid& valueId) {
        FiberMutex::ScopedLock lk(mutex_);
        pendingIds_.erase(valueId);
        MORDOR_LOG_TRACE(g_log) << this << " buffer notify(" << valueId << "), buffered=" << pendingIds_.size();
        if(pendingIds_.size() + 1 == bufferSize_) {
            MORDOR_LOG_INFO(g_log) << this << " buffer ready for new values";
            canPush_.set();
        }
    }

private:
    const size_t bufferSize_;
    BlockingQueue<Value>::ptr submitQueue_;
    set<Guid> pendingIds_;

    FiberEvent canPush_;
    FiberMutex mutex_;
};


Value createValue(const Guid& valueId, uint64_t snapshotId, uint64_t position, const char* data, size_t dataLength) {
    boost::shared_ptr<string> valueData(new string);
    SnapshotStreamData streamData;
    streamData.set_snapshot_id(snapshotId);
    streamData.set_position(position);
    if(dataLength > 0) {
        streamData.set_data(data, dataLength);
    }
    streamData.SerializeToString(valueData.get());
    if(valueData->length() > Value::kMaxValueSize) {
        MORDOR_LOG_ERROR(g_log) << " bad value " << valueId << " with " << valueData->length() << " bytes";
    }
    return Value(valueId, valueData);
}

void readData(SubmitBuffer* submitBuffer,
              uint64_t snapshotId,
              IOManager* ioManager)
{
    GuidGenerator g;
    const size_t kChunkSize = Value::kMaxValueSize - 2 * sizeof(uint64_t) - 2;
    StdinStream inputStream(*ioManager);

    char buffer[kChunkSize];
    uint64_t position = 0;
    uint64_t startT = TimerManager::now();
    while(true) {
        size_t bytes = inputStream.read(buffer, kChunkSize);
        Guid valueId = g.generate();

        Value v = createValue(valueId, snapshotId, position, buffer, bytes);
        submitBuffer->pushValue(v);

        if(bytes == 0) {
            break;
        }
        position += bytes;
    }
    // XXX hack
    submitBuffer->pushValue(Value(Guid(), boost::shared_ptr<string>(new string)));
    uint64_t timeElapsed = TimerManager::now() - startT;
    cout << "Read " << position << " bytes in " << timeElapsed << "us, " << int(position / (timeElapsed / 1000000.)) << " bps" << endl;
}

void writeToSocket(Socket::ptr s, const char* data, size_t length) {
    size_t sent = 0;
    while(sent < length) {
        sent += s->send(data + sent, length - sent);
    }
}

void submitValues(Socket::ptr s, BlockingQueue<Value>::ptr submitQueue, SubmitBuffer* submitBuffer) {
    uint64_t startT = TimerManager::now();
    size_t bytesSent = 0;
    size_t totalValueLength = 0;
    while(true) {
        Value v = submitQueue->pop();
        MORDOR_LOG_TRACE(g_log) << " submit popped " << v.valueId();
        if(v.valueId().empty()) {
            cout << v.valueId() << endl;
            break;
        }
        totalValueLength += v.size();

        ValueData valueData;
        v.serialize(&valueData);
        FixedSizeHeaderData header;
        header.set_size(valueData.ByteSize());
        char wireData[header.ByteSize() + valueData.ByteSize()];
        header.SerializeToArray(wireData, header.ByteSize());
        valueData.SerializeToArray(wireData + header.ByteSize(), valueData.ByteSize());
        writeToSocket(s, wireData, header.ByteSize() + valueData.ByteSize());
        bytesSent += header.ByteSize() + valueData.ByteSize();
        submitBuffer->notify(v.valueId());
        MORDOR_LOG_TRACE(g_log) << " submit sent " << v.valueId() << ", sent=" << bytesSent;
    }
    uint64_t timeElapsed = TimerManager::now() - startT;
    cout << "done. " << totalValueLength << " value bytes, " << bytesSent << " wire bytes in " << timeElapsed << "us, avg throughput " << int(bytesSent / (timeElapsed / 1000000.)) << " bps" << endl;
}

int main(int argc, char **argv) {
    Config::loadFromEnvironment();
    const size_t kBufferSize = 10000;
    if(argc < 3) {
        cout << " usage: send master_addr:port snapshot_id" << endl;
        return 1;
    }
    const uint64_t snapshotId = boost::lexical_cast<uint64_t>(argv[2]); 
    try {
        IOManager ioManager;
        Address::ptr masterAddress = Address::lookup(argv[1], AF_INET).front();
        Socket::ptr s = masterAddress->createSocket(ioManager, SOCK_STREAM);
        s->connect(masterAddress);

        BlockingQueue<Value>::ptr queue(new BlockingQueue<Value>("snapshot_stream"));
        SubmitBuffer submitBuffer(kBufferSize, queue);

        ioManager.schedule(boost::bind(readData, &submitBuffer, snapshotId, &ioManager));
        ioManager.schedule(boost::bind(submitValues, s, queue, &submitBuffer));
        ioManager.dispatch();
    } catch(...) {
        cout << boost::current_exception_diagnostic_information();
    }
    return 0;
}
