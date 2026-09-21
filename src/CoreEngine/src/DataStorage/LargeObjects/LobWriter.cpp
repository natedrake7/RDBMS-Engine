#include "../../../include/DataStorage/LargeObjects/LobWriter.h"
#include "../../../../Systemic/include/Memory/IAllocator.h"
#include "DataStorage/Table.h"

namespace CoreEngine::StorageTypes{
    void LobWriter::InitializeRootView(){
        this->_rootView = this->_reservation.Next<Pages::LobRootView>();
        this->_rootPageId = this->_rootView.PageId();
    }

    void LobWriter::OpenDataPage(){
        this->_currentDataView = this->_reservation.Next<Pages::LobDataView>();
        this->_currentBytesUsed = 0;
        this->_currentDataView.Initialize(this->_rootPageId, this->_dataPageCount);
    }

    void LobWriter::CloseDataPage(){
        this->AddDataPageId(this->_currentDataView.PageId());
        this->_currentDataView.Unpin();
        this->_dataPageCount++;
    }

    void LobWriter::RollToNewIndexPage(){
        if (
            !this->_currentIndexView.IsValid()
            || this->_currentIndexView.ChildCount() == Pages::LobLayout::INDEX_FANOUT
        ){
            this->_currentIndexView = this->_reservation.Next<Pages::LobIndexView>();
            this->_currentIndexView.Initialize(this->_rootPageId);
            this->_rootChildren[this->_rootChildrenCount++] = this->_currentIndexView.PageId();
        }
    }

    void LobWriter::AddDataPageId(const page_id_t pageId){
        if (this->_hasTwoLevels){
            this->AddPageToIndex(pageId);
            return;
        }

        if (this->_rootChildrenCount < Pages::LobLayout::ROOT_FANOUT){
            if (this->_rootChildren == nullptr)
                this->_rootChildren = static_cast<page_id_t*>(this->_allocator->AllocateRaw(LobWriter::ROOT_CHILDREN_SIZE));

            this->_rootChildren[this->_rootChildrenCount++] = pageId;
            return;
        }

        this->PromoteToTwoLevels();
        this->AddPageToIndex(pageId);
    }

    void LobWriter::AddPageToIndex(const page_id_t pageId){
        this->RollToNewIndexPage();
        this->_currentIndexView.AppendChild(pageId);
    }

    void LobWriter::BulkAppendPagesToIndex(const page_id_t* pages, UnsignedInt count){
        while (count > 0){
            this->RollToNewIndexPage();
            const auto toAppend = Math::Min<UnsignedInt>(
                count,
                Pages::LobLayout::INDEX_FANOUT - this->_currentIndexView.ChildCount()
            );
            this->_currentIndexView.BulkAppendChildren(pages, toAppend);
            pages += toAppend;
            count -= toAppend;
        }
    }

    void LobWriter::PromoteToTwoLevels(){
        auto* indexIds = static_cast<page_id_t*>(this->_allocator->AllocateRaw(LobWriter::INDEX_CHILDREN_SIZE));

        const auto count = this->_rootChildrenCount;

        const auto* __restrict__ rootChildren = this->_rootChildren;
        this->_rootChildren = indexIds;
        this->_hasTwoLevels = true;
        this->_rootChildrenCount = 0;

        this->BulkAppendPagesToIndex(rootChildren, count);
    }

    void LobWriter::WriteToPages(const object_t* data, UnsignedInt length){
        while (length > 0){
            if (!this->_currentDataView.IsValid())
                this->OpenDataPage();

            const auto bytesToWrite = static_cast<UnsignedInt>(Math::Min<UnsignedBigInt>(
                    length,
                    Pages::LobLayout::DATA_CAPACITY - this->_currentBytesUsed
                )
            );

            this->_currentDataView.WriteInline(this->_currentBytesUsed, data, bytesToWrite);
            this->_currentBytesUsed += bytesToWrite;

            data += bytesToWrite;
            length -= bytesToWrite;

            if (this->_currentBytesUsed == Pages::LobLayout::DATA_CAPACITY)
                this->CloseDataPage();
        }
    }

    void LobWriter::SpillStage(){
        this->_isStaging = false;
        if (this->_length > 0)
            this->WriteToPages(this->_stage, this->_length);
    }

    void LobWriter::ReleaseMemory(){
        if (this->_allocationStep._chunkAddress == nullptr)
            return;

        this->_allocator->ReleaseFromAllocationStep(this->_allocationStep);
        this->_stage = nullptr;
        this->_rootChildren = nullptr;
    }

    LobWriter::LobWriter(
        const ::Memory::IAllocator* allocator,
        const Table* table,
        const UnsignedBigInt lengthHint
    ):  _rootPageId(INVALID_PAGE_ID), _stage(nullptr),
        _isStaging(true), _currentBytesUsed(0), _dataPageCount(0),
        _rootChildren(nullptr), _rootChildrenCount(0), _hasTwoLevels(false),
        _length(0), _finished(false), _allocator(allocator),
        _allocationStep(allocator->RecordAllocationStart()){
        this->_reservation = lengthHint
                                 ? table->ReserveExtents(this->_allocator, PagesFor(lengthHint))
                                 : table->LazyReservation(this->_allocator);

        this->InitializeRootView();
    }

    LobWriter::~LobWriter(){
        this->ReleaseMemory();
        if (!this->_finished){
            // TODO: free root/index/data pages once a page-free API exists.
            assert(std::uncaught_exceptions() > 0 && "LobWriter destroyed without Finish() and without an exception");
        }
    }

    void LobWriter::Append(
        const object_t* data,
        const UnsignedInt length
    ){
        assert(!this->_finished);

        if (length > Pages::LobLayout::MAX_LENGTH - this->_length)
            throw std::runtime_error("LobWriter::Append(): data too large");

        if (this->_isStaging){
            if (this->_stage == nullptr)
                this->_stage = static_cast<object_t*>(this->_allocator->AllocateRaw(LobWriter::STAGING_SIZE));

            if (this->_length + length <= Pages::LobLayout::INLINE_CAPACITY){
                std::memcpy(this->_stage + this->_length, data, length);
                this->_length += length;
                return;
            }

            this->SpillStage();
        }

        this->WriteToPages(data, length);
        this->_length += length;
    }

    LobReference LobWriter::Finish(){
        assert(!this->_finished);

        this->_rootView.Initialize(this->_length);
        if (this->_isStaging){
            if (this->_length > 0)
                this->_rootView.WriteInline(0, this->_stage, this->_length);
        }
        else{
            if (this->_currentDataView.IsValid())
                this->CloseDataPage();

            assert(this->_rootView.Level() == (1 + this->_hasTwoLevels));
            this->_rootView.BulkAppendChildren(this->_rootChildren, this->_rootChildrenCount);
        }

        assert(this->_rootView.IsConsistent());

        this->_rootView.Unpin();
        this->_currentIndexView.Unpin();

        this->_finished = true;
        this->ReleaseMemory();
        return LobReference(this->_rootPageId, this->_length);
    }

    LobReference LobWriter::Write(
        const ::Memory::IAllocator* allocator,
        const Table* table,
        const object_t* data,
        const UnsignedInt length
    ){
        LobWriter writer(allocator, table, length);
        writer.Append(data, length);
        return writer.Finish();
    }
}
