#pragma once
#include <cassert>

#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/BufferPool/StorageLayout.h>
#include <Systemic/DataTypes/DataTypes.h>

namespace CoreEngine::Pages{
    struct LobRootHeader{
        UInt64_t _totalLength;
        page_id_t _companionRoot;
        UInt32_t _childCount;

        UInt8_t _level;
        UInt8_t _flags;

        UInt16_t _reserved0;
        UInt32_t _reserved1;

        LobRootHeader() = default;
    };

    static_assert(sizeof(LobRootHeader) == StorageLayout::LOB_ROOT_HEADER_SIZE);

    struct LobIndexHeader{
        page_id_t _rootPageId;
        UInt32_t _childCount;

        LobIndexHeader() = default;
    };

    static_assert(sizeof(LobIndexHeader) == StorageLayout::LOB_INDEX_HEADER_SIZE);

    struct LobDataHeader{
        page_id_t _rootPageId;
        UInt32_t _pageIndex;

        LobDataHeader() = default;
    };

    static_assert(sizeof(LobDataHeader) == StorageLayout::LOB_DATA_HEADER_SIZE);

    namespace LobLayout{
        inline constexpr UInt32_t ROOT_PAYLOAD = StorageLayout::PAGE_SIZE - StorageLayout::PAGE_HEADER_SIZE - StorageLayout::LOB_ROOT_HEADER_SIZE;
        inline constexpr UInt32_t INDEX_PAYLOAD = StorageLayout::PAGE_SIZE - StorageLayout::PAGE_HEADER_SIZE - StorageLayout::LOB_INDEX_HEADER_SIZE;
        inline constexpr UInt32_t DATA_CAPACITY = StorageLayout::PAGE_SIZE - StorageLayout::PAGE_HEADER_SIZE - StorageLayout::LOB_DATA_HEADER_SIZE;

        inline constexpr UInt32_t INLINE_CAPACITY = ROOT_PAYLOAD;
        inline constexpr UInt32_t ROOT_FANOUT = ROOT_PAYLOAD / sizeof(page_id_t);
        inline constexpr UInt32_t INDEX_FANOUT = INDEX_PAYLOAD / sizeof(page_id_t);

        inline constexpr UInt64_t TREE_CAPACITY = static_cast<UInt64_t>(ROOT_FANOUT) * INDEX_FANOUT * DATA_CAPACITY;
        inline constexpr UInt64_t MAX_LENGTH = 2 * Constants::GB;

        [[nodiscard]] inline constexpr UInt32_t DataPageCount(const UInt64_t length){
            return static_cast<UInt32_t>((length + DATA_CAPACITY - 1) / (DATA_CAPACITY));
        }

        [[nodiscard]] inline constexpr UInt8_t LevelFor(const UInt64_t length){
            if (length <= INLINE_CAPACITY)
                return 0;
            if (DataPageCount(length) <= ROOT_FANOUT)
                return 1;

            return 2;
        }

        [[nodiscard]] inline constexpr UInt32_t IndexPageCount(const UInt64_t length){
            return LevelFor(length) == 2
                ? (DataPageCount(length) + INDEX_FANOUT - 1) / (INDEX_FANOUT)
                : 0;
        }

        [[nodiscard]] inline constexpr UInt32_t DataPageBytes(const UInt64_t length, const UInt32_t pageIndex){
            assert(length > INLINE_CAPACITY);

            const auto last = DataPageCount(length) - 1;
            return pageIndex == last
                ? static_cast<UInt32_t>(length - static_cast<UInt64_t>(last) * DATA_CAPACITY)
                : DATA_CAPACITY;
        }

        static_assert(MAX_LENGTH <= TREE_CAPACITY, "34 GB must fit in two levels");
        static_assert(IndexPageCount(MAX_LENGTH) <= ROOT_FANOUT, "34 GB must fit in two levels");

        struct Position{
            UInt32_t _dataPage;     // index of the data page (unused at level 0)
            UInt32_t _rootSlot;     // level 1: data page slot; level 2: index page slot
            UInt32_t _indexSlot;    // level 2 only: slot inside the index page
            UInt32_t _inPageOffset;       // byte offset inside the data page, or inside the root at level 0

            explicit constexpr Position() = default;
            explicit constexpr Position(
                const UInt32_t dataPage,
                const UInt32_t rootSlot,
                const UInt32_t indexSlot,
                const UInt32_t inPage
            ):  _dataPage(dataPage), _rootSlot(rootSlot),
                _indexSlot(indexSlot), _inPageOffset(inPage){}

            static constexpr Position Inline(const UInt64_t offset){
                return Position(0, 0, 0, static_cast<UInt32_t>(offset));
            }
        };

        [[nodiscard]] inline constexpr Position Locate(const UInt8_t level, const UInt64_t offset){
            if (level == 0)
                return Position::Inline(offset);

            const auto dataPage = static_cast<UInt32_t>(offset / DATA_CAPACITY);
            const auto inPageOffset = static_cast<UInt32_t>(offset % DATA_CAPACITY);

            if (level == 1)
                return Position(dataPage, dataPage, 0, inPageOffset);

            return Position(dataPage, dataPage / INDEX_FANOUT, dataPage % INDEX_FANOUT, inPageOffset);
        }

        static_assert(INLINE_CAPACITY == 8144 && DATA_CAPACITY == 8160);
        static_assert(ROOT_FANOUT == 2036 && INDEX_FANOUT == 2040);
        static_assert(LevelFor(0) == 0 && LevelFor(INLINE_CAPACITY) == 0 && LevelFor(INLINE_CAPACITY + 1) == 1);
        static_assert(LevelFor(static_cast<UInt64_t>(ROOT_FANOUT) * DATA_CAPACITY) == 1);
        static_assert(LevelFor(static_cast<UInt64_t>(ROOT_FANOUT) * DATA_CAPACITY + 1) == 2);
        static_assert(IndexPageCount(MAX_LENGTH) <= ROOT_FANOUT, "34 GB must fit in two levels");
        static_assert(DataPageBytes(DATA_CAPACITY + 1, 1) == 1);
        static_assert(Locate(2, static_cast<UInt64_t>(INDEX_FANOUT) * DATA_CAPACITY)._rootSlot == 1);

    }
}
