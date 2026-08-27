#pragma once
#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataTypes/PackedWord.h"

namespace CoreEngine {
    struct Snapshot;
}

namespace CoreEngine::StorageTypes{
    struct RID{
        // bit 0-2 of _flags = Storage source
        static constexpr UnsignedTinyInt  SOURCE_SHIFT = 0;
        static constexpr UnsignedSmallInt SOURCE_MASK  = 0x0007;   // low-aligned, 3 bits

        enum Source: UnsignedTinyInt{
            Table = 0,
            Version = 1,
            TemporaryDb = 2,
            Count
        };

        page_id_t _pageId;
        UnsignedSmallInt _index;
        UnsignedSmallInt _flags;

        RID();
        RID(page_id_t pageId, Int index);
        RID(page_id_t pageId, Int index, Source source);

        RID& operator=(const RID& other) = default;
        RID(const RID& other) = default;

        void SetSource(Source source);
        [[nodiscard]] Source GetSource() const;
    };

    struct RowHeader{
        transaction_id_t _createdTransactionId;
        transaction_id_t _deletedTransactionId;

        RID _versionRID;

        explicit RowHeader();
        [[nodiscard]] bool HasOldVersion()const { return this->_versionRID._pageId != INVALID_PAGE_ID; }

        [[nodiscard]] bool IsVisibleForTransaction(const Snapshot& snapshot)const;
    };

    struct RowEntry{
        UnsignedSmallInt _offset;
        UnsignedSmallInt _meta;

        static constexpr UnsignedSmallInt TYPE_MASK = 0xC000; // upper 2 bits (positioned)
        static constexpr UnsignedSmallInt SIZE_MASK = 0x3FFF; // lower 14 bits
        static constexpr UnsignedTinyInt  TYPE_SHIFT = 14;
        static constexpr UnsignedSmallInt TYPE_VALUE_MASK = TYPE_MASK >> TYPE_SHIFT; // 0x3, low-aligned

        enum EntryType : UnsignedSmallInt {
            INLINE = 0b00,
            NULLVAL = 0b01,
            OVERFLOWVAL = 0b10,
            LOB = 0b11
        };

        RowEntry() : _offset(0), _meta(0) {}
        RowEntry(
            const UnsignedSmallInt offset,
            const EntryType type,
            const UnsignedSmallInt size
        ) : _offset(offset), _meta(EncodeMeta(type, size)){}

        [[nodiscard]] EntryType Type() const {
            return PackedWord<UnsignedSmallInt>::ExtractBits<EntryType, TYPE_SHIFT, TYPE_VALUE_MASK>(this->_meta);
        }

        [[nodiscard]] UnsignedSmallInt Size() const {
            return PackedWord<UnsignedSmallInt>::ExtractBits<UnsignedSmallInt, 0, SIZE_MASK>(this->_meta);
        }

        [[nodiscard]] UnsignedSmallInt Offset() const {
            return this->_offset;
        }

        static UnsignedSmallInt EncodeMeta(const EntryType type, const UnsignedSmallInt size){
            UnsignedSmallInt meta = 0;
            PackedWord<UnsignedSmallInt>::SetBits<0, SIZE_MASK>(&meta, size);
            PackedWord<UnsignedSmallInt>::SetBits<TYPE_SHIFT, TYPE_VALUE_MASK>(&meta, type);
            return meta;
        }

        static bool IsNull(const EntryType type){
            return type == EntryType::NULLVAL;
        }

        static bool IsOverflow(const EntryType type){
            return type == EntryType::OVERFLOWVAL;
        }
        static bool IsLOB(const EntryType type){
            return type == EntryType::LOB;
        }

        [[nodiscard]] bool IsNull() const { return this->Type() == EntryType::NULLVAL; }
        [[nodiscard]] bool IsOverflow() const { return this->Type() == EntryType::OVERFLOWVAL; }
        [[nodiscard]] bool IsLOB() const { return this->Type() == EntryType::LOB; }
    };
}