#pragma once
#include "../../DatabaseConstants.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"

namespace Pages{
    struct LobRootHeader{
        UnsignedBigInt _totalLength;
        page_id_t _companionRoot;
        UnsignedInt _childCount;

        UnsignedTinyInt _level;
        UnsignedTinyInt _flags;

        UnsignedSmallInt _reserved0;
        UnsignedInt _reserved1;

        LobRootHeader() = default;
    };

    static_assert(sizeof(LobRootHeader) == Constants::LOB_ROOT_HEADER_SIZE);

    struct LobIndexHeader{
        page_id_t _rootPageId;
        UnsignedInt _childCount;

        LobIndexHeader() = default;
    };

    static_assert(sizeof(LobIndexHeader) == Constants::LOB_INDEX_HEADER_SIZE);

    struct LobDataHeader{
        page_id_t _rootPageId;
        UnsignedInt _pageIndex;

        LobDataHeader() = default;
    };

    static_assert(sizeof(LobDataHeader) == Constants::LOB_DATA_HEADER_SIZE);

    namespace LobLayout{
        inline constexpr UnsignedInt ROOT_PAYLOAD = Constants::PAGE_SIZE - Constants::PAGE_HEADER_SIZE - Constants::LOB_ROOT_HEADER_SIZE;
        inline constexpr UnsignedInt INDEX_PAYLOAD = Constants::PAGE_SIZE - Constants::PAGE_HEADER_SIZE - Constants::LOB_INDEX_HEADER_SIZE;
        inline constexpr UnsignedInt DATA_CAPACITY = Constants::PAGE_SIZE - Constants::PAGE_HEADER_SIZE - Constants::LOB_DATA_HEADER_SIZE;

        inline constexpr UnsignedInt INLINE_CAPACITY = ROOT_PAYLOAD;
        inline constexpr UnsignedInt ROOT_FANOUT = ROOT_PAYLOAD / sizeof(page_id_t);
        inline constexpr UnsignedInt INDEX_FANOUT = INDEX_PAYLOAD / sizeof(page_id_t);

        inline constexpr UnsignedBigInt MAX_LENGTH = 2ull * Constants::GB;

        [[nodiscard]] inline constexpr UnsignedInt DataPageCount(const UnsignedBigInt length){
            return static_cast<UnsignedInt>((length + DATA_CAPACITY - 1) / (DATA_CAPACITY));
        }

        [[nodiscard]] inline constexpr UnsignedTinyInt LevelFor(const UnsignedBigInt length){
            if (length <= INLINE_CAPACITY)
                return 0;
            if (DataPageCount(length) <= ROOT_FANOUT)
                return 1;

            return 2;
        }

        [[nodiscard]] inline constexpr UnsignedInt IndexPageCount(const UnsignedBigInt length){
            return LevelFor(length) == 2
                ? (DataPageCount(length) + INDEX_FANOUT - 1) / (INDEX_FANOUT)
                : 0;
        }

        [[nodiscard]] inline constexpr UnsignedInt DataPageBytes(const UnsignedBigInt length, const UnsignedInt pageIndex){
            const auto last = DataPageCount(length) - 1;
            return pageIndex == last
                ? static_cast<UnsignedInt>(length - static_cast<UnsignedBigInt>(last) * DATA_CAPACITY)
                : DATA_CAPACITY;
        }

        struct Position{
            UnsignedInt _dataPage;     // index of the data page (unused at level 0)
            UnsignedInt _rootSlot;     // level 1: data page slot; level 2: index page slot
            UnsignedInt _indexSlot;    // level 2 only: slot inside the index page
            UnsignedInt _inPageOffset;       // byte offset inside the data page, or inside the root at level 0

            constexpr Position() = default;
            constexpr Position(
                const UnsignedInt dataPage,
                const UnsignedInt rootSlot,
                const UnsignedInt indexSlot,
                const UnsignedInt inPage
            ):  _dataPage(dataPage), _rootSlot(rootSlot),
                _indexSlot(indexSlot), _inPageOffset(inPage){}

            static constexpr Position Inline(const UnsignedBigInt offset){
                return Position(0, 0, 0, static_cast<UnsignedInt>(offset));
            }
        };

        [[nodiscard]] inline constexpr Position Locate(const UnsignedTinyInt level, const UnsignedBigInt offset){
            if (level == 0)
                return Position::Inline(offset);

            const auto dataPage = static_cast<UnsignedInt>(offset / DATA_CAPACITY);
            const auto inPageOffset = static_cast<UnsignedInt>(offset % DATA_CAPACITY);

            if (level == 1)
                return Position(dataPage, dataPage, 0, inPageOffset);

            return Position(dataPage, dataPage / INDEX_FANOUT, dataPage % INDEX_FANOUT, inPageOffset);
        }

        static_assert(INLINE_CAPACITY == 8160 && DATA_CAPACITY == 8176);
        static_assert(ROOT_FANOUT == 2040 && INDEX_FANOUT == 2044);
        static_assert(LevelFor(0) == 0 && LevelFor(INLINE_CAPACITY) == 0 && LevelFor(INLINE_CAPACITY + 1) == 1);
        static_assert(LevelFor(static_cast<UnsignedBigInt>(ROOT_FANOUT) * DATA_CAPACITY) == 1);
        static_assert(LevelFor(static_cast<UnsignedBigInt>(ROOT_FANOUT) * DATA_CAPACITY + 1) == 2);
        static_assert(IndexPageCount(MAX_LENGTH) <= ROOT_FANOUT, "34 GB must fit in two levels");
        static_assert(DataPageBytes(DATA_CAPACITY + 1, 1) == 1);
        static_assert(Locate(2, static_cast<UnsignedBigInt>(INDEX_FANOUT) * DATA_CAPACITY)._rootSlot == 1);

    }
}
