#pragma once
#include <Systemic/Constants.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataStructures/PolymorphicArray.h>

#include "CoreEngine/Accessors/PageAccessor.h"

namespace CoreEngine{
    class Database;
}

namespace CoreEngine::StorageTypes{
    class Table;

    struct AllocationCursor{
        extent_id_t _extentId;
        UnsignedInt _offset;

        AllocationCursor()
            : _extentId(INVALID_EXTENT_ID), _offset(0) {}
        AllocationCursor(const extent_id_t extentId, const UnsignedInt offset)
            : _extentId(extentId), _offset(offset) {}

        ~AllocationCursor() = default;
    };

    struct ExtentSegment{
        extent_id_t _firstExtentId;
        UnsignedInt _count;

        friend bool operator>(const ExtentSegment& lhs, const ExtentSegment& rhs){
            return lhs._count > rhs._count;
        }
    };

    class ExtentReservation{
        DataStructures::PolymorphicArray<ExtentSegment> _segments;
        Database* _db;
        const Table* _table;

        UnsignedSmallInt _segmentIndex;
        UnsignedSmallInt _segmentOffset;
        UnsignedSmallInt _extentIndex;

        Accessors::PageAccessor _pageAccessor;

        public:
            explicit ExtentReservation();
            explicit ExtentReservation(
                const ::Memory::IAllocator* allocator,
                Database* db,
                const Table* table
            );
            explicit ExtentReservation(
                DataStructures::PolymorphicArray<ExtentSegment>& segments,
                Database* db,
                const Table* table
            );

            ExtentReservation(const ExtentReservation& other) = delete;
            ExtentReservation& operator=(const ExtentReservation& other) = delete;
            ExtentReservation(ExtentReservation&& other) noexcept;
            ExtentReservation& operator=(ExtentReservation&& other) noexcept;

            [[nodiscard]] bool HasNext() const;

            template<typename TView>
            [[nodiscard]] TView Next();
    };
}
