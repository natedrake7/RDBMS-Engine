#pragma once
#include <Systemic/DataTypes/LobReference.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/Pages/LargeObjects/LobIndexView.h>
#include <CoreEngine/Pages/LargeObjects/LobRootView.h>

#include "CoreEngine/Accessors/PageAccessor.h"
#include "CoreEngine/Pages/LargeObjects/LobDataView.h"

namespace CoreEngine::StorageTypes{
    enum class LobReadStatus : UInt8_t{
        Ok = 0,
        Corrupt = 1
    };

    class LobReader{
        Accessors::PageAccessor _pageAccessor;
        DataTypes::LobReference _reference;

        Pages::LobRootView _rootView;
        Pages::LobIndexView _indexView;

        UInt32_t _indexRootSlot;

        UInt64_t _length;
        UInt64_t _position;               // for sequential Read()
        UInt8_t _level;
        LobReadStatus _status;

        [[nodiscard]] bool InitializeRootView();
        [[nodiscard]] bool InitializeIndexView(UInt32_t rootSlot);
        [[nodiscard]] page_id_t DataPageId(const Pages::LobLayout::Position& position);
        [[nodiscard]] Pages::LobDataView FetchDataPage(const Pages::LobLayout::Position& position);

        public:
            LobReader(Accessors::PageAccessor pageAccessor, const DataTypes::LobReference& reference);

            LobReader(const LobReader&) = delete;
            LobReader& operator=(const LobReader&) = delete;
            LobReader(LobReader&&) noexcept = default;
            LobReader& operator=(LobReader&&) noexcept = default;

            [[nodiscard]] UInt64_t Length()const{
                return this->_length;
            }

            [[nodiscard]] UInt64_t Position()const{
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

            [[nodiscard]] UInt32_t ReadAt(UInt64_t offset, object_t* buffer, UInt32_t size);
            [[nodiscard]] UInt32_t Read(object_t* buffer, UInt32_t size);

    };
}
