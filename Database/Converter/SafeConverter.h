#ifndef SAFECONVERTER_H
#define SAFECONVERTER_H

#include <string>
#include <limits>
#include <stdexcept>

using namespace std;

template<typename T>
class SafeConverter {
    public:
        static T SafeStoi(const string& input);
};

#endif //SAFECONVERTER_H
