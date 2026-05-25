#pragma once
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine{
    struct SelectionVector{
        UnsignedInt* selectedRids[Constants::MAX_QUERY_JOINS];

        UnsignedTinyInt* nullMask[Constants::MAX_QUERY_JOINS];

        UnsignedTinyInt sourceMap[Constants::MAX_QUERY_JOINS];

        Int selectedRidsCount;
        bool isIdentity;

        SelectionVector()
            :   selectedRids{nullptr}, nullMask{nullptr},
                sourceMap{0}, selectedRidsCount(0), isIdentity(false) {}

        void AllocateRids(const ::Memory::IAllocator* allocator, Int index, Int size);
        void AllocateNullMask(const ::Memory::IAllocator* allocator, Int index, Int size);
    };

    enum class DataVectorKind : UnsignedTinyInt{
        Flat = 0,
        Constant = 1,
        Dictionary = 2
    };

    struct DataVector{
        object_t* _data;
        UnsignedBigInt* _validity;

        Int _count;

        DataType _type;
        DataVectorKind _kind;

        DataVector();
    };
}
