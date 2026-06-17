#pragma once
#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine {
    struct Snapshot;
}


namespace CoreEngine::StorageTypes{
    struct RID{
        page_id_t _pageId;
        Int _index;

        RID();
        RID(page_id_t pageId, Int index);
    };

    struct RowHeader{
        transaction_id_t _createdTransactionId;
        transaction_id_t _deletedTransactionId;

        RID _oldVersionRID;

        explicit RowHeader();
        [[nodiscard]] bool HasOldVersion()const { return this->_oldVersionRID._pageId != INVALID_PAGE_ID; }
        [[nodiscard]] bool IsVisibleForTransaction(const Snapshot& snapshot)const;
        [[nodiscard]] bool IsDeletedForTransaction(const Snapshot& snapshot)const;
    };

    struct RowEntry{
        UnsignedSmallInt _offset;
        UnsignedSmallInt _meta;

        static constexpr UnsignedSmallInt TYPE_MASK = 0xC000; // upper 2 bits
        static constexpr UnsignedSmallInt SIZE_MASK = 0x3FFF; // lower 14 bits

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
        ) : _offset(offset){
            this->_meta = EncodeMeta(type, size);
        }

        [[nodiscard]] EntryType Type() const {
            return static_cast<EntryType>((this->_meta & TYPE_MASK) >> 14);
        }

        [[nodiscard]] UnsignedSmallInt Size() const {
            return this->_meta & SIZE_MASK;
        }

        [[nodiscard]] UnsignedSmallInt Offset() const {
            return this->_offset;
        }

        static UnsignedSmallInt EncodeMeta(const EntryType type, const UnsignedSmallInt size){
            return (static_cast<UnsignedSmallInt>(type) << 14) | (size & SIZE_MASK);
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