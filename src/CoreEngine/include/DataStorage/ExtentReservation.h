#pragma once
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../BufferPool/StorageManager.h"
#include "../Pages/IndexPageView.h"

namespace CoreEngine::StorageTypes{
    struct ExtentSegment{
        extent_id_t _firstExtentId;
        Int _count;
    };

    class ExtentReservation{
        DataStructures::PolymorphicArray<ExtentSegment> _segments;
        UnsignedSmallInt _segmentIndex;
        UnsignedSmallInt _segmentOffset;
        UnsignedSmallInt _extentIndex;
        Storage::FileKey _fileKey;

        public:
            ExtentReservation(DataStructures::PolymorphicArray<ExtentSegment>& segments, const Storage::FileKey fileKey)
                : _segments(std::move(segments)), _segmentIndex(0), _segmentOffset(0), _extentIndex(0), _fileKey(fileKey){}
            template<typename TView>
            [[nodiscard]] TView Next();
            [[nodiscard]] bool HasNext() const;
    };

    inline bool ExtentReservation::HasNext() const{
        return this->_segmentIndex < this->_segments.Size();
    }

    template <typename TView>
    TView ExtentReservation::Next(){
        const auto& [_firstPageId, _count] = this->_segments[this->_segmentIndex];

        const page_id_t pageId = _firstPageId + this->_segmentOffset++;

        if (this->_segmentOffset == _count){
            this->_segmentIndex++;
            this->_segmentOffset = 0;
        }

        if constexpr (std::is_same_v<TView, Pages::PageView>){
            return Storage::StorageManager::Get().CreatePage(this->_fileKey, pageId);
        }
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>){
            return Storage::StorageManager::Get().CreateIndexPage(this->_fileKey, pageId);
        }
        else
            static_assert(false, "Invalid Page type specified on extent reservation");

        return TView();
    }
}
