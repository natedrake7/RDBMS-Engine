#include "../../include/DataStorage/ExtentReservation.h"

#include "Database.h"
#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Pages/IndexPageView.h"
#include "Guards/WriterGuard.h"

namespace CoreEngine::StorageTypes{
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
            const auto subReserve = this->_db->ReserveExtents(
                this->_segments.GetAllocator(),
                Constants::EXTENT_SIZE,
                this->_tableOrdinalPos
            );

            for (const auto& segment: subReserve._segments)
                this->_segments.Push(segment);
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

    bool ExtentReservation::HasNext() const{
        return this->_segmentIndex < this->_segments.Size();
    }
}
