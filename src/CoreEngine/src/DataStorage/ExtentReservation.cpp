#include "../../include/DataStorage/ExtentReservation.h"

#include "Database.h"
#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Pages/IndexPageView.h"
#include "Guards/WriterGuard.h"

namespace CoreEngine::StorageTypes{
    ExtentReservation::ExtentReservation()
        :   _segments(), _db(nullptr), _segmentIndex(0),
            _segmentOffset(0), _extentIndex(0), _tableOrdinalPos(0){}

    ExtentReservation::ExtentReservation(
        const ::Memory::IAllocator* allocator,
        Database* db,
        const table_id_t tableOrdinalPos
    )   : _segments(allocator), _db(db), _segmentIndex(0),
          _segmentOffset(0), _extentIndex(0), _tableOrdinalPos(tableOrdinalPos){}

    ExtentReservation::ExtentReservation(
        DataStructures::PolymorphicArray<ExtentSegment>& segments,
        Database* db,
        const table_id_t tableOrdinalPos
    ):   _segments(std::move(segments)), _db(db), _segmentIndex(0),
        _segmentOffset(0), _extentIndex(0), _tableOrdinalPos(tableOrdinalPos){}

    template <typename TView>
    TView ExtentReservation::Next(){
        if (this->_segmentIndex == this->_segments.Size()){
            return this->_db->LazyAllocateSinglePage<TView>(this->_segments.GetAllocator(), this->_tableOrdinalPos);
            // const auto subReserve = this->_db->ReserveExtents(
            //     this->_segments.GetAllocator(),
            //     Constants::EXTENT_SIZE,
            //     this->_tableOrdinalPos
            // );
            //
            // for (const auto& segment: subReserve._segments)
            //     this->_segments.Push(segment);
        }

        const auto& [_firstExtentId, _count] = this->_segments[this->_segmentIndex];

        const page_id_t pageId = (_firstExtentId + this->_segmentOffset) * Constants::EXTENT_SIZE + this->_extentIndex++;

        if (this->_extentIndex == Constants::EXTENT_SIZE){
            this->_segmentOffset++;
            this->_extentIndex = 0;
        }

        if (this->_segmentOffset == _count){
            this->_segmentIndex++;
            this->_segmentOffset = 0;
        }

        auto page = Storage::StorageManager::Get().CreatePage<TView>(this->_db->DataFileKey(), pageId);

        {
            const auto pfs = this->_db->GetAssociatedPfsPage(this->_db->SystemFileKey(), pageId);
            MultiThreading::WriterGuard lock(&pfs.Latch());
            pfs.SetPageMetaData(&page);
        }

        return page;
    }

    template Pages::PageView ExtentReservation::Next<Pages::PageView>();
    template Pages::IndexPageView ExtentReservation::Next<Pages::IndexPageView>();
    template Pages::OverflowPageView ExtentReservation::Next<Pages::OverflowPageView>();
    template Pages::LargeObjectView ExtentReservation::Next<Pages::LargeObjectView>();

    ExtentReservation::ExtentReservation(ExtentReservation&& other) noexcept
        : _segments(std::move(other._segments)), _db(other._db), _segmentIndex(other._segmentIndex),
          _segmentOffset(other._segmentOffset), _extentIndex(other._extentIndex), _tableOrdinalPos(other._tableOrdinalPos){}

    ExtentReservation& ExtentReservation::operator=(ExtentReservation&& other) noexcept{
        if (this == &other)
            return *this;

        this->_segments = std::move(other._segments);
        this->_db = other._db;
        this->_segmentIndex = other._segmentIndex;
        this->_segmentOffset = other._segmentOffset;
        this->_extentIndex = other._extentIndex;
        this->_tableOrdinalPos = other._tableOrdinalPos;

        other._db = nullptr;
        other._segments.Clear();
        other._segmentIndex = 0;
        other._segmentOffset = 0;
        other._extentIndex = 0;
        other._tableOrdinalPos = 0;

        return *this;
    }

    bool ExtentReservation::HasNext() const{
        return this->_segmentIndex < this->_segments.Size();
    }
}
