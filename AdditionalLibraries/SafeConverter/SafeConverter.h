﻿#pragma once

#include <string>
#include <limits>
#include <stdexcept>
#include <cstdint>

using namespace std;

template<typename T>
class SafeConverter {
public:
    static T SafeStoi(const string& input);
};

