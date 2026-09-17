#pragma once
#include <string>
#include <vector>

#include "DataTypes/DataTypes.h"

namespace Network
{
    class PayloadReader;
}

enum class DataType : uint8_t;

namespace Client{
    class ResultDecoder{
        static constexpr auto MAX_SPAN = std::numeric_limits<UnsignedInt>::max();

        template<typename T>
        [[nodiscard]] static bool Load(
            const char* values,
            const UnsignedSmallInt width,
            const UnsignedInt row,
            T& output
        ){
            if (width != sizeof(T))
                return false;

            std::memcpy(&output, values + row * sizeof(T), sizeof(T));
            return true;
        }

        struct ColumnDescription{
            std::string _name;
            DataType _type;
        };

        struct ColumnView{
            const char* _validity;
            const char* _data;
            const char* _offsets;

            UnsignedInt _blobSize;
            UnsignedSmallInt _width;

            DataType _type;
            bool _isConstant;


            ColumnView()
                :   _validity(nullptr),
                    _data(nullptr)
                  , _offsets(nullptr)
                  , _blobSize(0)
                  , _width(0)
                  , _type(DataType::Null)
                  , _isConstant(false)
            {}
        };

        std::vector<ColumnDescription> _columns;
        std::vector<ColumnView> _columnViews;
        UnsignedInt _rowCount;
        UnsignedInt _columnCount;


        [[nodiscard]] static bool DecodeVariableLengthColumn(Network::PayloadReader& reader, ColumnView& columnView, UnsignedInt encodedRows);
        [[nodiscard]] static bool DecodeFixedLengthColumn(Network::PayloadReader& reader, ColumnView& columnView, UnsignedInt encodedRows);

        static void PrintValue(std::ostream& os, const ColumnView& columnView, UnsignedInt row);

        public:
            ResultDecoder() = default;

            [[nodiscard]] bool DecodeRowDescription(const std::vector<char>* payload);
            [[nodiscard]] bool DecodeDataBatch(const std::vector<char>* payload);

            void PrintHeader(std::ostream& os)const;
            void PrintBatch(std::ostream& os)const;


    };
}
