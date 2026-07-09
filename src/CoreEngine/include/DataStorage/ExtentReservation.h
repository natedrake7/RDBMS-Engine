#pragma once
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "./../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace CoreEngine{
    class Database;
}

namespace CoreEngine::StorageTypes{
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
        UnsignedSmallInt _segmentIndex;
        UnsignedSmallInt _segmentOffset;
        UnsignedSmallInt _extentIndex;
        table_id_t _tableOrdinalPos;

        public:
            ExtentReservation();
            ExtentReservation(
                const ::Memory::IAllocator* allocator,
                Database* db,
                table_id_t tableOrdinalPos
            );
            ExtentReservation(
                DataStructures::PolymorphicArray<ExtentSegment>& segments,
                Database* db,
                table_id_t tableOrdinalPos
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
