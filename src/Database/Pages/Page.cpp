#include "Page.h"
#include "../Database.h"
#include "./LargeObject/LargeObjectPage.h"
#include "../../Systemic/DataStructures/BitMap/BitMap.h"
#include "../../Systemic/MultiThreading/Guards/WriterGuard/WriterGuard.h"
#include "../Block/Block.h"
#include "../Storage/StorageManager/StorageManager.h"

#include <cstring>

using namespace DatabaseEngine::StorageTypes;

namespace Pages
{
    PageHeader::PageHeader()
    {
        this->pageType = PageType::DATA;
        this->pageId = INVALID_PAGE_ID;
        this->pageSize = 0;
        this->bytesLeft = static_cast<Constants::page_size_t>(PAGE_SIZE - Constants::PAGE_HEADER_SIZE);
    }

    PageHeader::~PageHeader() = default;

    Page::Page(const page_id_t &pageId, const bool &isPageCreation)
    {
        this->header.pageId = pageId;
        this->isDirty = isPageCreation;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->header.pageType = PageType::DATA;
        this->priority = Constants::PagePriority::LOW;
    }

    Page::Page()
    {
        this->isDirty = false;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->header.pageType = PageType::DATA;
        this->priority = Constants::PagePriority::LOW;
    }

    Page::Page(const PageHeader &pageHeader)
    {
        this->header = pageHeader;
        this->isDirty = false;
        this->pinCount = 0;
        this->hasSecondChance = true;
        this->logSequenceNumber = 0;
        this->priority = Constants::PagePriority::LOW;
    }

    Page::~Page()
    {
        for (const auto &row : this->rows)
            delete row;
    }

    void Page::InsertRow(Row *row, int* indexPosition){
        this->rows.push_back(row);
        
        if(indexPosition != nullptr)
            *indexPosition = this->rows.size() - 1;

        this->header.bytesLeft -= row->GetTotalRowSize();
        this->header.pageSize++;
        this->isDirty = true;
    }

    void Page::InsertRow(Row *row, const int& indexPosition){
        this->rows.insert(this->rows.begin() + indexPosition, row);
        this->header.bytesLeft -= row->GetTotalRowSize();
        this->header.pageSize++;
        this->isDirty = true;
    }

    void Page::ReadFromDisk(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr){
        if (table == nullptr) {
            for (int i = 0; i < this->header.pageSize; i++)
                this->rows.push_back(Page::ReadRowFromDisk(data, offSet));

            return;
        }

        const auto &columns = table->GetColumns();

        for (int i = 0; i < this->header.pageSize; i++)
            this->rows.push_back(Page::ReadRowFromDisk(data, table, offSet, columns));
    }

    Row* Page::ReadRowFromDisk(const vector<char>& data, const Table *table, page_offset_t &offSet, const vector<Column*>& columns){
        auto*  row = new Row(*table);

        row->ReadHeaderFromDisk(data, offSet);
        row->ReadVersionHeaderFromDisk(data, offSet);
        row->ReadDataFromDisk(data, offSet, columns);

        return row;
    }

    DatabaseEngine::StorageTypes::Row * Page::ReadRowFromDisk(const vector<char> &data, page_offset_t &offSet) {
        auto*  row = new Row();

        row->ReadHeaderFromDisk(data, offSet);
        row->ReadVersionHeaderFromDisk(data, offSet);
        row->ReadDataFromDisk(data, offSet);

        return row;
    }

    void Page::WriteRowToDisk(fstream* filePtr, const Row* row){
        row->WriteHeaderToDisk(filePtr);
        row->WriteVersionHeaderToDisk(filePtr);
        row->WriteDataToDisk(filePtr);
    }

    void Page::WritePageHeaderToDisk(fstream *filePtr) const{
        filePtr->write(reinterpret_cast<const char *>(&this->header.pageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.pageSize), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.bytesLeft), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.pageType), sizeof(PageType));
    }


    void Page::WriteToDisk(fstream *filePtr){
        this->WritePageHeaderToDisk(filePtr);

        for (const auto &row : this->rows)
            Page::WriteRowToDisk(filePtr, row);
    }

    // void Page::Delete(vector<Row*> &deletedRows, const Expressions::Expression *expression){
    //     Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow);
    //
    //     for (int i = 0; i < this->rows.size(); i++) {
    //         auto* row = this->rows[i];
    //
    //         context.row = row;
    //
    //         const auto value = expression->Evaluate(context);
    //         if (value.GetBool()) {
    //             this->rows.erase(this->rows.begin() + i);
    //             i--;
    //
    //             //remove it from index as well
    //
    //             deletedRows.push_back(row);
    //
    //             //deleted row, mark it as dirty
    //             this->isDirty = true;
    //         }
    //     }
    //
    //     this->header.pageSize = this->rows.size();
    // }

    // void Page::Delete(const Expressions::Expression *expression){
    //     Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::SingleRow);
    //
    //     for (int i = 0; i < this->rows.size(); i++) {
    //         auto* row = this->rows[i];
    //
    //         context.row = row;
    //         const auto value = expression->Evaluate(context);
    //         if (value.GetBool()) {
    //             this->rows.erase(this->rows.begin() + i);
    //             i--;
    //
    //             RowHeader *rowHeader = row->GetHeader();
    //
    //             for (const auto &block : row->GetData())
    //             {
    //                 if (rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
    //                 {
    //                     DataObjectPointer objectPointer;
    //                     memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));
    //                 }
    //             }
    //           //delete row to deallocate space
    //           delete row;
    //
    //           //deleted row, mark it as dirty
    //           this->isDirty = true;
    //         }
    //     }
    //
    //     this->header.pageSize = this->rows.size();
    //     this->UpdateBytesLeft();
    // }

    void Page::Delete(const int &indexPosition) {
        const auto* row = this->rows[indexPosition];

        this->rows.erase(this->rows.begin() + indexPosition);

        delete row;

        this->UpdateBytesLeft();
        this->isDirty = true;
        this->header.pageSize--;
    }

    void Page::SetFileName(const string &filename) { this->filename = filename; }

    void Page::SetPageId(const page_id_t &pageId) { this->header.pageId = pageId; }

    void Page::UpdatePageSize() { this->header.pageSize = this->rows.size(); }

    void Page::UpdateBytesLeft()
    {
        this->header.bytesLeft = Constants::PAGE_SIZE_WITHOUT_HEADER;

        for (const auto &row : this->rows)
            this->header.bytesLeft -= row->GetTotalRowSize();

        this->header.pageSize = this->rows.size();
        this->isDirty = true;
    }

    void Page::UpdateBytesLeft(const row_size_t& previousRowSize, const row_size_t& currentRowSize)
    {
        this->header.bytesLeft += static_cast<int64_t>(currentRowSize) -static_cast<int64_t>(previousRowSize);

        this->isDirty = true;
    }

    const string &Page::GetFileName() const { return this->filename; }

    const page_id_t &Page::GetPageId() const { return this->header.pageId; }

    const bool &Page::IsDirty() const { return this->isDirty; }

    const page_size_t &Page::GetBytesLeft() const { return this->header.bytesLeft; }

    void Page::SetDirty(){ this->isDirty = true; }

    void Page::SetLogSequenceNumber(const log_sequence_number_t &logSequenceNumber){
        this->logSequenceNumber = logSequenceNumber;
    }

    const log_sequence_number_t & Page::GetLogSequenceNumber() const{ return this->logSequenceNumber; }

    page_size_t Page::GetPageSize() const { return this->header.pageSize; }

    const PageType &Page::GetPageType() const { return this->header.pageType; }

    int Page::GetRows(
        std::vector<const Row*> *result,
        const size_t &rowsToSelect,
        const int32_t& startingPosition
    ) const
    {
        if (startingPosition >= this->rows.size())
            return -1;

        for (int i = startingPosition; i < this->rows.size(); i++) {
            result->push_back(this->rows.at(i));

            if (result->size() == rowsToSelect)
                return i;
        }

        return static_cast<int>(this->rows.size() - 1);
    }

    void Page::GetRowByIndex(vector<Row>* rows, const Table &table, const int &indexPosition) const
    {
        const auto &row = this->rows[indexPosition];

        const RowHeader *rowHeader = row->GetHeader();

        vector<Block *> copyBlocks = row->GetBlockCopies();

        rows->emplace_back(table, copyBlocks, rowHeader->nullBitMap);
    }

    const Row * Page::GetRow(const int &indexPosition)const { return this->rows.at(indexPosition); }

    vector<DatabaseEngine::StorageTypes::Row *>* Page::GetDataRowsUnsafe() { return &this->rows; }

    void Page::IncreasePinCount() {
        this->pinCount.fetch_add(1, std::memory_order_relaxed);
    }

    void Page::DecreasePinCount() {
        this->pinCount.fetch_sub(1, std::memory_order_relaxed);
    }

    int Page::GetPinCount() const {
        return this->pinCount.load(std::memory_order_relaxed);
    }

    Constants::PagePriority Page::GetPriority() const {
        return this->priority.load(std::memory_order_relaxed);
    }

    bool Page::HasSecondChance() const{
        return this->hasSecondChance;
    }

    void Page::SetHasSecondChanceUnsafe(const bool &secondChance) {
        this->hasSecondChance = secondChance;
    }

    void Page::UniqueLock()const {
        this->latch.UniqueLock();
    }

    void Page::UniqueUnlock() const{
        this->latch.UniqueUnlock();
    }

    void Page::SharedLock() const{
        this->latch.SharedLock();
    }

    void Page::SharedUnlock() const {
        this->latch.SharedUnlock();
    }

    MultiThreading::ReadWriteMutex & Page::GetLatch() const {
        return this->latch;
    }
}