#pragma once
#include <Systemic/DataTypes/DataTypes.h>

namespace CoreEngine::Pages{
    struct RawRowReference{
        object_t* _data;
        Int32_t size;

        RawRowReference(object_t* data, const Int32_t size){
            this->_data = data;
            this->size = size;
        }
    };
}
