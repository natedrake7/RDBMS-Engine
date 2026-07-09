#include "../../../include/DataStorage/Table.h"
#include "../../../include/Database.h"
#include "../../../include/BufferPool/StorageManager.h"
#include "DataStorage/SerializedRow.h"

namespace CoreEngine::StorageTypes {
    page_id_t Table::InsertLargeObject(const ::Memory::IAllocator* allocator, const Value& value) const{
        const auto numOfPages = Math::Ceil<Int>(
            static_cast<float>(value.Size()) / static_cast<float>(Constants::LARGE_DATA_OBJECT_SIZE)
        );

        ExtentReservation reservation;
        if (numOfPages == 1)
            reservation = this->LazyReservation(allocator);
        else{
            const auto numExtents = Math::Ceil<Int>(static_cast<float>(numOfPages) / Constants::EXTENT_SIZE);
            reservation = this->ReserveExtents(allocator, numExtents);
        }

        return this->StoreLargeObject(reservation, value);
    }

    page_id_t Table::StoreLargeObject(
        ExtentReservation& reservation,
        const Value& value
    )const{
        const auto* data = value.Data();
        block_size_t remaining = value.Size();

        page_id_t headPageId = INVALID_PAGE_ID;
        Pages::LargeObjectView previous;

        while (true){
            auto page = reservation.Next<Pages::LargeObjectView>();

            const auto chunk = Math::Min<block_size_t>(remaining, Constants::LARGE_DATA_OBJECT_SIZE);
            page.SetData(data, chunk);

            const auto pfs = Database::GetAssociatedPfsPage(this->GetSystemFileKey(), page.PageId());
            pfs.SetPageMetaData(&page);

            if (headPageId == INVALID_PAGE_ID)
                headPageId = page.PageId();
            if (previous.IsValid())
                previous.SetNextPageId(page.PageId());

            remaining -= chunk;
            data += chunk;

            if (remaining == 0)
                break;

            previous = std::move(page);
        }

        return headPageId;
    }
}
