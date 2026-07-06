#include "../../include/DataStorage/ExtentReservation.h"

#include "Database.h"
#include "../../include/BufferPool/StorageManager.h"
#include "../../include/Pages/IndexPageView.h"

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

        const auto pfs = this->_db->GetAssociatedPfsPage(this->_db->SystemFileKey(), pageId);

        if constexpr (std::is_same_v<TView, Pages::PageView>){
            auto page = Storage::StorageManager::Get().CreatePage(this->_db->DataFileKey(), pageId);
            pfs.SetPageMetaData(&page);
            return page;
        }
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>){
            auto page = Storage::StorageManager::Get().CreateIndexPage(this->_db->DataFileKey(), pageId);
            pfs.SetPageMetaData(&page);
            return page;
        }
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
