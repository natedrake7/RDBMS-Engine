#pragma once
#include "../../../../Systemic/include/DataTypes/DataTypes.h"

namespace Pages{
    struct RawRowReference{
        object_t* _data;
        Int size;

        RawRowReference(object_t* data, const Int size){
            this->_data = data;
            this->size = size;
        }
    };
}
