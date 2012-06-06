#include "value.h"
#include "proto/rpc_messages.pb.h"
#include <mordor/iomanager.h>
#include <mordor/socket.h>
#include <boost/lexical_cast.hpp>
#include <iostream>

using namespace lightning;
using namespace paxos;
using namespace Mordor;
using namespace std;

void writeToSocket(Socket::ptr s, const char* data, size_t length) {
    size_t sent = 0;
    while(sent < length) {
        sent += s->send(data + sent, length - sent);
    }
}

void submitValues(Socket::ptr s, size_t n) {
    GuidGenerator g;
    uint64_t startT = TimerManager::now();
    size_t bytesSent = 0;
    for(size_t i = 0; i < n; ++i) {
        boost::shared_ptr<string> data(new string(8000, ' '));
        Guid valueId = g.generate();
        Value v(valueId, data);
        ValueData valueData;
        v.serialize(&valueData);

        FixedSizeHeaderData header;
        header.set_size(valueData.ByteSize());
        char wireData[header.ByteSize() + valueData.ByteSize()];
        header.SerializeToArray(wireData, header.ByteSize());
        valueData.SerializeToArray(wireData + header.ByteSize(), valueData.ByteSize());
        writeToSocket(s, wireData, header.ByteSize() + valueData.ByteSize());
        bytesSent += header.ByteSize() + valueData.ByteSize();
    }
    uint64_t timeElapsed = TimerManager::now() - startT;
    cout << "done, sent " << bytesSent << " bytes in " << timeElapsed << "us, avg throughput " << int(bytesSent / (timeElapsed / 1000000.)) << " bps" << endl;
}

int main(int argc, char **argv) {
    if(argc < 3) {
        cout << " usage: send master_addr:port n" << endl;
        return 1;
    }
    const size_t instances = boost::lexical_cast<size_t>(argv[2]);
    try {
        IOManager ioManager;
        Address::ptr masterAddress = Address::lookup(argv[1], AF_INET).front();
        Socket::ptr s = masterAddress->createSocket(ioManager, SOCK_STREAM);
        s->connect(masterAddress);
        ioManager.schedule(boost::bind(submitValues, s, instances));
        ioManager.dispatch();
    } catch(...) {
        cout << boost::current_exception_diagnostic_information();
    }
    return 0;
}
