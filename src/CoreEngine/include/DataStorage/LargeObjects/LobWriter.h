#pragma once
#include <array>
#include "../LobReference.h"
#include "../ExtentReservation.h"
#include "../../Pages/LargeObjects/LobRootView.h"
#include "../../Pages/LargeObjects/LobIndexView.h"
#include "../../Pages/LargeObjects/LobDataView.h"

namespace CoreEngine::StorageTypes{
    class Table;

    class LobWriter{
        static constexpr UnsignedInt STAGING_SIZE = Pages::LobLayout::INLINE_CAPACITY;
        static constexpr UnsignedInt ROOT_CHILDREN_SIZE = sizeof(page_id_t) * Pages::LobLayout::ROOT_FANOUT;
        static constexpr UnsignedInt INDEX_CHILDREN_SIZE = sizeof(page_id_t) * Pages::LobLayout::INDEX_FANOUT;

        [[nodiscard]] static constexpr Int PagesFor(const UnsignedBigInt length){
            if (Pages::LobLayout::LevelFor(length) == 0)
                return 1;

            return static_cast<Int>(1 + Pages::LobLayout::IndexPageCount(length) + Pages::LobLayout::DataPageCount(length));
        }

        ExtentReservation _reservation;

        Pages::LobRootView _rootView;

        page_id_t _rootPageId;

        object_t* _stage;
        bool _isStaging;

        Pages::LobDataView _currentDataView;
        UnsignedInt _currentBytesUsed;
        UnsignedInt _dataPageCount;

        page_id_t* _rootChildren;
        UnsignedInt _rootChildrenCount;

        Pages::LobIndexView _currentIndexView;
        bool _hasTwoLevels;

        UnsignedBigInt _length;
        bool _finished;

        const ::Memory::IAllocator* _allocator;
        Memory::AllocationStep _allocationStep;

        void InitializeRootView();
        void OpenDataPage();
        void CloseDataPage();

        void RollToNewIndexPage();

        void AddDataPageId(page_id_t pageId);
        void AddPageToIndex(page_id_t pageId);
        void BulkAppendPagesToIndex(const page_id_t* pages, UnsignedInt count);

        void PromoteToTwoLevels();

        void WriteToPages(const object_t* data, UnsignedInt length);


        void SpillStage();

        void ReleaseMemory();

        public:
            LobWriter(const ::Memory::IAllocator* allocator, const Table* table, UnsignedBigInt lengthHint);
            ~LobWriter();

            void Append(
                const object_t* data,
                UnsignedInt length
            );

            [[nodiscard]] LobReference Finish();

            [[nodiscard]] UnsignedBigInt Length()const{
                return this->_length;
            }

            [[nodiscard]] static LobReference Write(
                const ::Memory::IAllocator* allocator,
                const Table* table,
                const object_t* data,
                UnsignedInt length
            );
    };
}