#include <CoreEngine/DataStorage/ExtentReservation.h>

#include <CoreEngine/Database.h>
#include <CoreEngine/Pages/IndexPageView.h>
#include <Systemic/Guards/WriterGuard.h>

#include "CoreEngine/Pages/OverflowPageView.h"
#include "CoreEngine/Pages/LargeObjects/LobDataView.h"
#include "CoreEngine/Pages/LargeObjects/LobIndexView.h"
#include "CoreEngine/Pages/LargeObjects/LobRootView.h"

namespace CoreEngine::StorageTypes{
    ExtentReservation::ExtentReservation()
        :   _segments(), _db(nullptr), _table(nullptr),
            _segmentIndex(0), _segmentOffset(0), _extentIndex(0),
            _pageAccessor(){}

    ExtentReservation::ExtentReservation(
        const ::Memory::IAllocator* allocator,
        Database* db,
        const Table* table
    )   :   _segments(allocator), _db(db), _table(table),
            _segmentIndex(0), _segmentOffset(0), _extentIndex(0),
            _pageAccessor(db->GetPageAccessor()){}

    ExtentReservation::ExtentReservation(
        DataStructures::PolymorphicArray<ExtentSegment>& segments,
        Database* db,
        const Table* table
    ):   _segments(std::move(segments)), _db(db), _table(table),
        _segmentIndex(0), _segmentOffset(0), _extentIndex(0),
        _pageAccessor(db->GetPageAccessor()){}

    template <typename TView>
    TView ExtentReservation::Next(){
        if (this->_segmentIndex == this->_segments.Size()){
            return this->_db->LazyAllocateTablePage<TView>(
                this->_segments.GetAllocator(),
                this->_table
            );
        }

        const auto& [_firstExtentId, _count] = this->_segments[this->_segmentIndex];

        const page_id_t pageId = (_firstExtentId + this->_segmentOffset) * StorageLayout::EXTENT_SIZE + this->_extentIndex++;

        if (this->_extentIndex == StorageLayout::EXTENT_SIZE){
            this->_segmentOffset++;
            this->_extentIndex = 0;
        }

        if (this->_segmentOffset == _count){
            this->_segmentIndex++;
            this->_segmentOffset = 0;
        }

        auto page = this->_pageAccessor.CreatePage<TView>(pageId);

        {
            const auto pfs = this->_pageAccessor.GetAssociatedPfsPage(pageId);
            MultiThreading::WriterGuard lock(&pfs.Latch());
            pfs.SetPageMetaData(&page);
        }

        return page;
    }

    template Pages::PageView ExtentReservation::Next<Pages::PageView>();
    template Pages::IndexPageView ExtentReservation::Next<Pages::IndexPageView>();
    template Pages::OverflowPageView ExtentReservation::Next<Pages::OverflowPageView>();

    template Pages::LobRootView ExtentReservation::Next<Pages::LobRootView>();
    template Pages::LobIndexView ExtentReservation::Next<Pages::LobIndexView>();
    template Pages::LobDataView ExtentReservation::Next<Pages::LobDataView>();

    ExtentReservation::ExtentReservation(ExtentReservation&& other) noexcept
        :   _segments(std::move(other._segments)), _db(other._db), _table(other._table),
            _segmentIndex(other._segmentIndex), _segmentOffset(other._segmentOffset),
            _extentIndex(other._extentIndex), _pageAccessor(other._pageAccessor){}

    ExtentReservation& ExtentReservation::operator=(ExtentReservation&& other) noexcept{
        if (this == &other)
            return *this;

        this->_segments = std::move(other._segments);
        this->_db = other._db;
        this->_segmentIndex = other._segmentIndex;
        this->_segmentOffset = other._segmentOffset;
        this->_extentIndex = other._extentIndex;
        this->_table = other._table;
        this->_pageAccessor = other._pageAccessor;

        other._db = nullptr;
        other._segments.Clear();
        other._segmentIndex = 0;
        other._segmentOffset = 0;
        other._extentIndex = 0;
        other._table = nullptr;

        return *this;
    }

    bool ExtentReservation::HasNext() const{
        return this->_segmentIndex < this->_segments.Size();
    }
}
