#pragma once
#include "../LobReference.h"
#include "../../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../BufferPool/StorageManager.h"
#include "../../Pages/LargeObjects/LobIndexView.h"
#include "../../Pages/LargeObjects/LobRootView.h"

namespace CoreEngine::StorageTypes{
    enum class LobReadStatus : UnsignedTinyInt{
        Ok = 0,
        Corrupt = 1
    };

    class LobReader{
        Storage::FileKey _fileKey;
        LobReference _reference;

        Pages::LobRootView _rootView;
        Pages::LobIndexView _indexView;

        UnsignedInt _indexRootSlot;

        UnsignedBigInt _length;
        UnsignedBigInt _position;               // for sequential Read()
        UnsignedTinyInt _level;
        LobReadStatus _status;

        [[nodiscard]] bool InitializeRootView();
        [[nodiscard]] bool InitializeIndexView(UnsignedInt rootSlot);
        [[nodiscard]] page_id_t DataPageId(const Pages::LobLayout::Position& position);
        [[nodiscard]] Pages::LobDataView FetchDataPage(const Pages::LobLayout::Position& position);

        public:
            LobReader(const Storage::FileKey& fileKey, const LobReference& reference);

            LobReader(const LobReader&) = delete;
            LobReader& operator=(const LobReader&) = delete;
            LobReader(LobReader&&) noexcept = default;
            LobReader& operator=(LobReader&&) noexcept = default;

            [[nodiscard]] UnsignedBigInt Length()const{
                return this->_length;
            }

            [[nodiscard]] UnsignedBigInt Position()const{
                return this->_position;
            }

            [[nodiscard]] bool IsAtEnd()const{
                return this->_position >= this->_length;
            }

            [[nodiscard]] bool IsOk()const{
                return this->_status == LobReadStatus::Ok;
            }

            [[nodiscard]] LobReadStatus Status()const{
                return this->_status;
            }

            [[nodiscard]] UnsignedInt ReadAt(UnsignedBigInt offset, object_t* buffer, UnsignedInt size);
            [[nodiscard]] UnsignedInt Read(object_t* buffer, UnsignedInt size);

    };
}
