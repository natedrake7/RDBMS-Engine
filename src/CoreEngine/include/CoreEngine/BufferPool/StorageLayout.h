#pragma once
#include <cstddef>
#include <limits>

#include <Systemic/DataTypes/DataTypes.h>

/**
 * @name Storage layout
 * Everything that decides where a page lives and how big it is. Page-type specific header sizes live next
 * to their header structs (GlobalAllocationPageView.h, IndexPageStructs.h, LobPageStructs.h), each pinned
 * with a static_assert on the struct's size.
 * @{
 */

namespace CoreEngine::StorageLayout{
        // Bump on any change to the page or file layout: a file written with another version is refused on open
    inline constexpr UnsignedInt FILE_FORMAT_VERSION = 1;

    /**
     * @name Page and extent geometry
     * @{
     */
    inline constexpr size_t PAGE_SIZE = 8 * 1024;
    inline constexpr UnsignedSmallInt PAGE_HEADER_SIZE = sizeof(log_sequence_number_t)
            + sizeof(UnsignedInt) + sizeof(page_id_t)
            + 2 * sizeof(page_size_t) + sizeof(UnsignedBigInt);
    inline constexpr page_size_t PAGE_SIZE_WITHOUT_HEADER = PAGE_SIZE - PAGE_HEADER_SIZE;

    inline constexpr size_t EXTENT_SIZE = 8;
    inline constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;
    /** @} End of: Page and extent geometry */

    /**
     * @name BufferPool segmenting
     * @{
     */
    typedef uint64_t frame_identity_t;
    typedef uint32_t frame_id_t;

    inline constexpr frame_id_t INVALID_FRAME_ID = std::numeric_limits<frame_id_t>::max();
    inline constexpr UnsignedInt MAX_BUFFER_POOL_SLOTS = 256;

    namespace PageAddress{
        inline constexpr UnsignedInt SEGMENT_BITS = 12;
        inline constexpr UnsignedInt GROUP_BITS = 10;
        inline constexpr UnsignedInt DIRECTORY_BITS = 10;

        inline constexpr UnsignedInt SEGMENT_SIZE = 1 << SEGMENT_BITS;
        inline constexpr UnsignedInt GROUP_SIZE = 1 << GROUP_BITS;
        inline constexpr UnsignedInt DIRECTORY_SIZE = 1 << DIRECTORY_BITS;

        [[nodiscard]] inline constexpr UnsignedInt SegmentSlot(const page_id_t pageId){
            return pageId >> PageAddress::SEGMENT_BITS;
        }

        [[nodiscard]] inline constexpr UnsignedInt PageSlot(const page_id_t pageId){
            return pageId & (PageAddress::SEGMENT_SIZE - 1);
        }

        [[nodiscard]] inline constexpr UnsignedInt DirectorySlot(const page_id_t pageId){
            const auto segmentId = PageAddress::SegmentSlot(pageId);
            return (segmentId >> PageAddress::GROUP_BITS) & (PageAddress::DIRECTORY_SIZE - 1);
        }

        [[nodiscard]] inline constexpr UnsignedInt GroupSlot(const page_id_t pageId){
            const auto segmentId = PageAddress::SegmentSlot(pageId);
            return segmentId & (PageAddress::GROUP_SIZE - 1);
        }
    }

    static_assert(
        PageAddress::SEGMENT_SIZE % CoreEngine::StorageLayout::EXTENT_SIZE == 0,
        "an extent never straddles two segments"
    );

    /** @} End of: BufferPool segmenting */


    /**
     * @name Allocation maps
     * A GAM page tracks EXTENT_BIT_MAP_SIZE extents (1 bit each); a PFS page tracks PAGE_FREE_SPACE_SIZE
     * pages (1 byte each).
     * @{
     */
    inline constexpr size_t GAM_BIT_MAP_SIZE = 64000;
    inline constexpr size_t GAM_BITMAP_BYTES = GAM_BIT_MAP_SIZE / EXTENT_SIZE;
    inline constexpr UnsignedInt GAM_NUMBER_OF_PAGES = GAM_BIT_MAP_SIZE * EXTENT_SIZE;
    inline constexpr UnsignedSmallInt PAGE_FREE_SPACE_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;

    static_assert(GAM_BIT_MAP_SIZE % EXTENT_SIZE == 0, "the GAM bitmap is a whole number of bytes");
    static_assert(PAGE_FREE_SPACE_SIZE % EXTENT_SIZE == 0, "an extent never straddles two PFS pages");
    /** @} End of: Allocation maps */

    inline constexpr page_size_t INDEX_PAGE_ADDITIONAL_HEADER_SIZE = 2 * sizeof(page_id_t) + sizeof(UnsignedTinyInt) + 3;
    inline constexpr page_size_t INDEX_PAGE_DEFAULT_SIZE = PAGE_SIZE_WITHOUT_HEADER - INDEX_PAGE_ADDITIONAL_HEADER_SIZE;


    // The GAM header fills the space between the page header and the bitmap, which ends the page
    inline constexpr size_t GAM_HEADER_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE - GAM_BIT_MAP_SIZE / 8;
    inline constexpr size_t GAM_HEADER_RESERVED_SPACE =
        GAM_HEADER_SIZE - sizeof(gam_index_t) - sizeof(UnsignedInt) - 2 * sizeof(extent_id_t);

    // Offset of the bitmap inside the page
    inline constexpr size_t GAM_METADATA_SIZE = PAGE_HEADER_SIZE + GAM_HEADER_SIZE;

    inline constexpr size_t LOB_ROOT_HEADER_SIZE =
        sizeof(UnsignedBigInt) + sizeof(page_id_t)
        + 2 * sizeof(UnsignedInt) + 2 * sizeof(UnsignedTinyInt)
        + sizeof(UnsignedSmallInt);

    inline constexpr size_t LOB_INDEX_HEADER_SIZE = sizeof(page_id_t) + sizeof(UnsignedInt);
    inline constexpr size_t LOB_DATA_HEADER_SIZE = sizeof(page_id_t) + sizeof(UnsignedInt);

    /**
     * @name System file layout
     * Page 0 is the header, then one fixed block per GAM range:
     *   [ GAM | PFS 0 | PFS 1 | ... | PFS 62 ]   (covering exactly GAM_NUMBER_OF_PAGES data pages)
     * PFS ranges restart at every GAM boundary, so every map page is pure arithmetic on the data page id.
     * @{
     */
    inline constexpr page_id_t HEADER_PAGE_ID = 0;
    inline constexpr page_id_t FIRST_INTERVAL_PAGE_ID = 1;

    inline constexpr page_id_t PFS_PER_GAM_INTERVAL = (GAM_NUMBER_OF_PAGES + PAGE_FREE_SPACE_SIZE - 1) / PAGE_FREE_SPACE_SIZE;
    inline constexpr page_id_t SYSTEM_PAGES_PER_INTERVAL = PFS_PER_GAM_INTERVAL + 1;

    // Last GAM whose whole range fits in page_id_t (its maximum is reserved for INVALID_PAGE_ID)
    inline constexpr gam_index_t MAX_GAM_INDEX = std::numeric_limits<page_id_t>::max() / GAM_NUMBER_OF_PAGES - 1;

    static_assert(
        static_cast<UnsignedBigInt>(MAX_GAM_INDEX + 1) * GAM_NUMBER_OF_PAGES - 1 < std::numeric_limits<page_id_t>::max(),
        "the last GAM range must fit in page_id_t"
    );

    [[nodiscard]] constexpr gam_index_t GamIndexOfPage(const page_id_t dataPageId){
        return dataPageId / GAM_NUMBER_OF_PAGES;
    }

    [[nodiscard]] constexpr gam_index_t GamIndexOfExtent(const extent_id_t extentId){
        return extentId / GAM_BIT_MAP_SIZE;
    }

    [[nodiscard]] constexpr extent_id_t GamBaseExtent(const gam_index_t gamIndex){
        return gamIndex * GAM_BIT_MAP_SIZE;
    }

    [[nodiscard]] constexpr page_id_t GamPageId(const gam_index_t gamIndex){
        return FIRST_INTERVAL_PAGE_ID + gamIndex * SYSTEM_PAGES_PER_INTERVAL;
    }

    [[nodiscard]] constexpr page_id_t pageSlotInInterval(const page_id_t dataPageId){
        return dataPageId % GAM_NUMBER_OF_PAGES;
    }

    [[nodiscard]] constexpr page_id_t PfsPageId(const page_id_t dataPageId){
        return StorageLayout::GamPageId(GamIndexOfPage(dataPageId)) + 1
            + pageSlotInInterval(dataPageId) / PAGE_FREE_SPACE_SIZE;
    }

    [[nodiscard]] constexpr UnsignedInt PfsSlot(const page_id_t pageId){
        return pageSlotInInterval(pageId) % PAGE_FREE_SPACE_SIZE;
    }

    [[nodiscard]] constexpr page_id_t ExtentFirstPageId(const extent_id_t extentId){
        return extentId * EXTENT_SIZE;
    }

    [[nodiscard]] constexpr extent_id_t ExtentId(const page_id_t pageId){
        return pageId / EXTENT_SIZE;
    }

    inline constexpr gam_index_t FIRST_GAM_INDEX = 0;
    inline constexpr page_id_t FIRST_PFS_PAGE_ID = PfsPageId(0);
    
    static_assert(GamPageId(0) == 1 && PfsPageId(0) == 2);
    static_assert(PfsPageId(GAM_NUMBER_OF_PAGES - 1) == 64);   // last PFS of interval 0
    static_assert(GamPageId(1) == 65 && PfsPageId(GAM_NUMBER_OF_PAGES) == 66);
}
