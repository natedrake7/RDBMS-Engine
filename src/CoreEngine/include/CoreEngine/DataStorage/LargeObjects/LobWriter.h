#pragma once
#include <array>
#include <Systemic/DataTypes/LobReference.h>
#include <CoreEngine/DataStorage/ExtentReservation.h>
#include <CoreEngine/Pages/LargeObjects/LobRootView.h>
#include <CoreEngine/Pages/LargeObjects/LobIndexView.h>
#include <CoreEngine/Pages/LargeObjects/LobDataView.h>

namespace CoreEngine::StorageTypes{
    class Table;

    class LobWriter{
        static constexpr UInt32_t STAGING_SIZE = Pages::LobLayout::INLINE_CAPACITY;
        static constexpr UInt32_t ROOT_CHILDREN_SIZE = sizeof(page_id_t) * Pages::LobLayout::ROOT_FANOUT;
        static constexpr UInt32_t INDEX_CHILDREN_SIZE = sizeof(page_id_t) * Pages::LobLayout::INDEX_FANOUT;

        [[nodiscard]] static constexpr Int32_t PagesFor(const UInt64_t length){
            if (Pages::LobLayout::LevelFor(length) == 0)
                return 1;

            return static_cast<Int32_t>(1 + Pages::LobLayout::IndexPageCount(length) + Pages::LobLayout::DataPageCount(length));
        }

        ExtentReservation _reservation;

        Pages::LobRootView _rootView;

        page_id_t _rootPageId;

        object_t* _stage;
        bool _isStaging;

        Pages::LobDataView _currentDataView;
        UInt32_t _currentBytesUsed;
        UInt32_t _dataPageCount;

        page_id_t* _rootChildren;
        UInt32_t _rootChildrenCount;

        Pages::LobIndexView _currentIndexView;
        bool _hasTwoLevels;

        UInt64_t _length;
        bool _finished;

        const ::Memory::IAllocator* _allocator;
        ::Memory::AllocationStep _allocationStep;

        void InitializeRootView();
        void OpenDataPage();
        void CloseDataPage();

        void RollToNewIndexPage();

        void AddDataPageId(page_id_t pageId);
        void AddPageToIndex(page_id_t pageId);
        void BulkAppendPagesToIndex(const page_id_t* pages, UInt32_t count);

        void PromoteToTwoLevels();

        void WriteToPages(const object_t* data, UInt32_t length);


        void SpillStage();

        void ReleaseMemory();

        public:
            LobWriter(const ::Memory::IAllocator* allocator, const Table* table, UInt64_t lengthHint);
            ~LobWriter();

            void Append(
                const object_t* data,
                UInt32_t length
            );

            [[nodiscard]] DataTypes::LobReference Finish();

            [[nodiscard]] UInt64_t Length()const{
                return this->_length;
            }

            [[nodiscard]] static DataTypes::LobReference Write(
                const ::Memory::IAllocator* allocator,
                const Table* table,
                const object_t* data,
                UInt32_t length
            );
    };
}