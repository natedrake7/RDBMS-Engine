#pragma once
#include <Systemic/Constants.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/PackedWord.h>

namespace CoreEngine {
    struct Snapshot;
}

namespace CoreEngine::StorageTypes{
    struct RID{
        // bit 0-2 of _flags = Storage source
        static constexpr UInt8_t  SOURCE_SHIFT = 0;
        static constexpr UInt16_t SOURCE_MASK  = 0x0007;   // low-aligned, 3 bits

        enum Source: UInt8_t{
            Table = 0,
            Version = 1,
            TemporaryDb = 2,
            Count
        };

        page_id_t _pageId;
        UInt16_t _index;
        UInt16_t _flags;

        RID();
        explicit RID(page_id_t pageId, Int32_t index);
        explicit RID(page_id_t pageId, Int32_t index, Source source);

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
        UInt16_t _offset;
        UInt16_t _meta;

        static constexpr UInt16_t TYPE_MASK = 0xC000; // upper 2 bits (positioned)
        static constexpr UInt16_t SIZE_MASK = 0x3FFF; // lower 14 bits
        static constexpr UInt8_t  TYPE_SHIFT = 14;
        static constexpr UInt16_t TYPE_VALUE_MASK = TYPE_MASK >> TYPE_SHIFT; // 0x3, low-aligned

        enum EntryType : UInt16_t {
            INLINE = 0b00,
            NULLVAL = 0b01,
            OVERFLOWVAL = 0b10,
            LOB = 0b11
        };

        RowEntry() : _offset(0), _meta(0) {}
        RowEntry(
            const UInt16_t offset,
            const EntryType type,
            const UInt16_t size
        ) : _offset(offset), _meta(EncodeMeta(type, size)){}

        [[nodiscard]] EntryType Type() const {
            return PackedWord<UInt16_t>::ExtractBits<EntryType, TYPE_SHIFT, TYPE_VALUE_MASK>(this->_meta);
        }

        [[nodiscard]] UInt16_t Size() const {
            return PackedWord<UInt16_t>::ExtractBits<UInt16_t, 0, SIZE_MASK>(this->_meta);
        }

        [[nodiscard]] UInt16_t Offset() const {
            return this->_offset;
        }

        static inline constexpr UInt16_t EncodeMeta(const EntryType type, const UInt16_t size){
            UInt16_t meta = 0;
            PackedWord<UInt16_t>::SetBits<0, SIZE_MASK>(&meta, size);
            PackedWord<UInt16_t>::SetBits<TYPE_SHIFT, TYPE_VALUE_MASK>(&meta, type);
            return meta;
        }

        static inline constexpr bool IsNull(const EntryType type){
            return type == EntryType::NULLVAL;
        }

        static inline constexpr bool IsOverflow(const EntryType type){
            return type == EntryType::OVERFLOWVAL;
        }
        static inline constexpr bool IsLOB(const EntryType type){
            return type == EntryType::LOB;
        }

        [[nodiscard]] bool IsNull() const { return this->Type() == EntryType::NULLVAL; }
        [[nodiscard]] bool IsOverflow() const { return this->Type() == EntryType::OVERFLOWVAL; }
        [[nodiscard]] bool IsLOB() const { return this->Type() == EntryType::LOB; }
    };
}