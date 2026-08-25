#pragma once
#include "../../CoreEngine/include/DatabaseConstants.h"

namespace QueryPipeline{
    struct ReferencedColumns{
        static constexpr size_t WORDS = Constants::MAX_TABLE_COLUMNS / 64;

        UnsignedBigInt _mask[Constants::MAX_QUERY_JOINS][WORDS];
        DataType _types[Constants::MAX_QUERY_JOINS][Constants::MAX_TABLE_COLUMNS];
        UnsignedSmallInt _count[Constants::MAX_QUERY_JOINS];

        void Add(
            const UnsignedSmallInt slotIndex,
            const column_index_t ordinalPosition,
            const DataType type
        ){
            auto* word = &this->_mask[slotIndex][ordinalPosition >> 6];
            const auto bit = static_cast<UnsignedTinyInt>(ordinalPosition & 63);

            if (PackedWord<UnsignedBigInt>::GetBit(*word, bit))
                return;

            this->_count[slotIndex]++;
            PackedWord<UnsignedBigInt>::SetBit(word, bit, true);
            this->_types[slotIndex][ordinalPosition] = type;
        }

        [[nodiscard]] bool Contains(const UnsignedSmallInt slotIndex, const column_index_t ordinalPosition) const{
            return PackedWord<UnsignedBigInt>::GetBit(
                this->_mask[slotIndex][ordinalPosition >> 6],
                ordinalPosition & 63
            );
        }

        [[nodiscard]] DataType GetType(const UnsignedSmallInt slotIndex, const column_index_t ordinalPosition) const{
            return this->_types[slotIndex][ordinalPosition];
        }

        [[nodiscard]] UnsignedSmallInt GetSlotCount(const UnsignedSmallInt slotIndex) const{
            return this->_count[slotIndex];
        }

        [[nodiscard]] bool IsSlotEmpty(const UnsignedSmallInt slotIndex) const{
            return this->_count[slotIndex] == 0;
        }

        template<typename TCallback>
        void ForEachColumn(const UnsignedSmallInt slotIndex, TCallback callback) const{
            for (auto word = 0; word < WORDS; word++){
                auto bits = this->_mask[slotIndex][word];

                while (bits != 0){
                    const auto ordinalPosition = static_cast<column_index_t>(
                        word * 64 + static_cast<size_t>(std::countr_zero(bits))
                    );

                    callback(ordinalPosition, this->_types[slotIndex][ordinalPosition]);
                    bits &= bits - 1;
                }
            }
        }

    };
}
