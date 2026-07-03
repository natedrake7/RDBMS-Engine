#include "../../include/DataStorage/ExtentReservation.h"

#include "Database.h"
#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Pages/IndexPageView.h"

namespace CoreEngine::StorageTypes{
    ExtentReservation::ExtentReservation(
        DataStructures::PolymorphicArray<ExtentSegment>& segments,
        Database* db,
        const table_id_t tableId
    ):   _segments(std::move(segments)), _db(db), _segmentIndex(0),
        _segmentOffset(0), _extentIndex(0), _tableId(tableId){}

    template <typename TView>
    TView ExtentReservation::Next(){
        if (this->_segmentIndex == this->_segments.Size()){
            const auto subReserve = this->_db->ReserveExtents(
                this->_segments.GetAllocator(),
                Constants::EXTENT_SIZE,
                this->_tableId
            );

            for (const auto& segment : subReserve._segments)
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




        if constexpr (std::is_same_v<TView, Pages::PageView>)
            return Storage::StorageManager::Get().CreatePage(this->_db->GetDataFileKey(), pageId);
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>)
            return Storage::StorageManager::Get().CreateIndexPage(this->_db->GetDataFileKey(), pageId);
        else
            static_assert(sizeof(TView) == 0, "Invalid Page type specified on extent reservation");

        return TView();
    }

    template Pages::PageView ExtentReservation::Next<Pages::PageView>();
    template Pages::IndexPageView ExtentReservation::Next<Pages::IndexPageView>();

    bool ExtentReservation::HasNext() const{
        return this->_segmentIndex < this->_segments.Size();
    }
}
