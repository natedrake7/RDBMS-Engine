#include "../../../include/DataStorage/LargeObjects/LobReader.h"

namespace CoreEngine::StorageTypes{
    bool LobReader::InitializeRootView(){
        this->_rootView = Storage::StorageManager::Get().GetPage<Pages::LobRootView>(
            this->_fileKey,
            this->_reference._rootPageId
        );

        if (!this->_rootView.IsValid()
            || this->_rootView.GetPageType() != Constants::PageType::LOB_ROOT
            || !this->_rootView.IsConsistent()
            || this->_rootView.TotalLength() != this->_reference._totalLength
        ) return false;

        this->_length = this->_rootView.TotalLength();
        this->_level = this->_rootView.Level();

        auto expected = 0;
        switch (this->_level){
        case 1:
            expected = Pages::LobLayout::DataPageCount(this->_length);
            break;
        case 2:
            expected = Pages::LobLayout::IndexPageCount(this->_length);
            break;
        default:
            expected = 0;
            break;
        }

        return this->_rootView.ChildCount() == expected;
    }

    bool LobReader::InitializeIndexView(const UnsignedInt rootSlot){
        if (this->_indexRootSlot == rootSlot)
            return true;

        if (rootSlot > this->_rootView.ChildCount())
            return false;

        this->_indexView = Storage::StorageManager::Get().GetPage<Pages::LobIndexView>(
            this->_fileKey,
            this->_rootView.Child(rootSlot)
        );

        this->_indexRootSlot = rootSlot;

        const auto dataPages = Pages::LobLayout::DataPageCount(this->_length);
        const auto expectedDataPages = Math::Min<UnsignedInt>(Pages::LobLayout::INDEX_FANOUT, dataPages - rootSlot * Pages::LobLayout::INDEX_FANOUT);

        return this->_indexView.IsValid()
            && this->_indexView.GetPageType() == Constants::PageType::LOB_INDEX
            && this->_indexView.RootPageId() == this->_reference._rootPageId
            && this->_indexView.ChildCount() == expectedDataPages;
    }

    page_id_t LobReader::DataPageId(const Pages::LobLayout::Position& position){
        switch (this->_level){
            case 1:
                if (position._rootSlot >= this->_rootView.ChildCount())
                    throw std::runtime_error("LobReader::DataPageId: Invalid root slot");

                return this->_rootView.Child(position._rootSlot);
            case 2:{
                if (!this->InitializeIndexView(position._rootSlot) || position._indexSlot >= this->_indexView.ChildCount())
                    throw std::runtime_error("LobReader::DataPageId: Invalid index slot");

                return this->_indexView.Child(position._indexSlot);
            }
            default:
                throw std::runtime_error("LobReader::DataPageId: Invalid level");
        }
    }

    Pages::LobDataView LobReader::FetchDataPage(const Pages::LobLayout::Position& position){
        const auto pageId = this->DataPageId(position);

        auto dataView = Storage::StorageManager::Get().GetPage<Pages::LobDataView>(this->_fileKey, pageId);

        if (
            !dataView.IsValid()
            || dataView.GetPageType() != Constants::PageType::LOB_DATA
            || dataView.RootPageId() != this->_reference._rootPageId
            || dataView.PageIndex() != position._dataPage
        ) throw std::runtime_error("LobReader::FetchDataPage: Invalid data page");

        return dataView;
    }

    LobReader::LobReader(const Storage::FileKey& fileKey, const LobReference& reference)
        :   _fileKey(fileKey), _reference(reference),
            _indexRootSlot(std::numeric_limits<UnsignedInt>::max()), _length(0),
            _position(0), _level(0), _status(LobReadStatus::Ok){
        if (!reference.IsValid() || !this->InitializeRootView())
            this->_status = LobReadStatus::Corrupt;
    }

    UnsignedInt LobReader::ReadAt(const UnsignedBigInt offset, object_t* buffer, UnsignedInt size){
        if (!this->IsOk() || offset >= this->_length)
            return 0;

        size = Math::Min<UnsignedBigInt>(size, this->_length - offset);

        if (this->_level == 0){
            std::memcpy(buffer, this->_rootView.InlineData() + offset, size);
            return size;
        }

        UnsignedInt copied = 0;
        while (copied < size){
            const auto position = Pages::LobLayout::Locate(this->_level, offset + copied);
            const auto dataPage = this->FetchDataPage(position);

            const auto available = Pages::LobLayout::DataPageBytes(this->_length, position._dataPage) - position._inPageOffset;
            const auto toCopy = Math::Min<UnsignedInt>(available, size - copied);

            std::memcpy(buffer + copied, dataPage.InlineData() + position._inPageOffset, toCopy);
            copied += toCopy;
        }

        return copied;
    }

    UnsignedInt LobReader::Read(object_t* buffer, const UnsignedInt size){
        const auto copied = this->ReadAt(this->_position, buffer, size);
        this->_position += copied;
        return copied;
    }
}
