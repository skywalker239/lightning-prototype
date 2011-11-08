#include <iostream>
#include <mordor/config.h>
#include <mordor/iomanager.h>
#include <mordor/sleep.h>
#include <mordor/socket.h>
#include <mordor/timer.h>

using namespace std;
using namespace Mordor;

const uint64_t kTimeout = 1000000;

void printInterfaces() {
    static uint64_t lastUs = 0;
    uint64_t currentUs = TimerManager::now();
    cout << currentUs << " " << currentUs - lastUs << endl;
    lastUs = currentUs;

    auto addrs = Address::getInterfaceAddresses();
    for(auto i = addrs.begin(); i != addrs.end(); ++i) {
        cout << i->first << " -> (" << *i->second.first << ", " << i->second.second << ")" << endl;
    }
}

int main(int , char** ) {
    Config::loadFromEnvironment();
    IOManager ioManager;
    Timer::ptr t = ioManager.registerTimer(1000000, printInterfaces, true);
    ioManager.dispatch();
    return 0;
}

