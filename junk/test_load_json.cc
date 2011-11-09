#include <iostream>
#include <mordor/streams/file.h>
#include <mordor/json.h>

using namespace std;
using namespace Mordor;

int main(int argc, char** argv) {
    if(argc != 3) {
        cout << "usage: load_json file.json" << endl;
    }

    try {
        FileStream input(argv[1], FileStream::READ);

        JSON::Value val = JSON::parse(input);
        cout << "Read value: " << val << endl;
        cout << val["baaz"] << endl;
    } catch(...) {
        cout << boost::current_exception_diagnostic_information() << endl;
    }
}

