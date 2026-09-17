#include "../../include/Decoding/ResultDecoder.h"
#include <ostream>

#include "../../../Server/include/ClientConnection.h"
#include "../../../Server/include/ResultFormat.h"
#include "DataTypes/DateTime.h"
#include "DataTypes/Guid.h"
#include "Network/PayloadReader.h"

namespace Client{
    bool ResultDecoder::DecodeVariableLengthColumn(
        Network::PayloadReader& reader,
        ColumnView& columnView,
        const UnsignedInt encodedRows
    ){
        const auto offSetBytes = (encodedRows + 1) * sizeof(UnsignedBigInt);

        if (offSetBytes > MAX_SPAN || !reader.ReadSpan(columnView._offsets, offSetBytes))
            return false;

        std::memcpy(&columnView._blobSize, columnView._offsets + encodedRows * sizeof(UnsignedBigInt), sizeof(UnsignedBigInt));

        if (!reader.ReadSpan(columnView._data, columnView._blobSize))
            return false;

        return true;
    }

    bool ResultDecoder::DecodeFixedLengthColumn(
        Network::PayloadReader& reader,
        ColumnView& columnView,
        const UnsignedInt encodedRows
    ){
        if (!reader.Read<UnsignedSmallInt>(columnView._width))
            return false;

        const auto valueBytes = columnView._width * encodedRows;
        if (valueBytes > MAX_SPAN || !reader.ReadSpan(columnView._data, valueBytes))
            return false;

        return true;
    }

    void ResultDecoder::PrintValue(std::ostream& os, const ColumnView& columnView, UnsignedInt row){
        switch (columnView._type){
        case DataType::Bool:{
            auto value = false;
            if (Load(columnView._data, columnView._width, row, value)){
                os << (value ? "TRUE" : "FALSE");
                return;
            }
            break;
        }
        case DataType::TinyInt:{
            TinyInt value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << static_cast<Int>(value); return;
            }
            break;
        }
        case DataType::SmallInt:{
            SmallInt value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << value; return;
            }
            break;
        }
        case DataType::Int:{
            Int value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << value; return;
            }
            break;
        }
        case DataType::BigInt:{
            BigInt value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << value; return;
            }
            break;
        }
        case DataType::DateTime:{
            BigInt timestamp = 0;       // milliseconds since the Unix epoch, UTC
            if (Load(columnView._data, columnView._width, row, timestamp)){
                DataTypes::DateTime(timestamp).Print(os);
                return;
            }
            break;
        }
        case DataType::Guid:{
            if (columnView._width != DataTypes::GUID_SIZE)
                break;

            os << DataTypes::Guid(reinterpret_cast<const object_t*>(columnView._data + row * columnView._width));
            return;
        }
        case DataType::String:
        case DataType::Json:
        case DataType::Decimal:{
            UnsignedInt begin = 0;
            UnsignedInt end = 0;
            std::memcpy(&begin, columnView._offsets + row * sizeof(UnsignedInt), sizeof(UnsignedInt));
            std::memcpy(&end,   columnView._offsets + (row + 1) * sizeof(UnsignedInt), sizeof(UnsignedInt));

            if (begin > end || end > columnView._blobSize)
                break;

            os.write(columnView._data + begin, end - begin);
            return;
        }
        default:
            break;
        }

        os << "<invalid>";
    }

    bool ResultDecoder::DecodeRowDescription(const std::vector<char>* payload){
        Network::PayloadReader reader(payload->data(), payload->size());

        Int columnCount = 0;
        if (!reader.Read<Int>(columnCount))
            return false;

        this->_columns.resize(columnCount);
        for (auto& column : this->_columns){
            if (!reader.ReadString(column._name) || !reader.Read<DataType>(column._type))
                return false;
        }

        return true;
    }

    bool ResultDecoder::DecodeDataBatch(const std::vector<char>* payload){
        Network::PayloadReader reader(payload->data(), payload->size());

        if (!reader.Read<UnsignedInt>(this->_rowCount) || !reader.Read<UnsignedInt>(this->_columnCount))
            return false;

        this->_columnViews.assign(this->_columnCount, ColumnView());
        for (auto& columnView : this->_columnViews){
            DataType type;
            UnsignedTinyInt flags;

            if (!reader.Read<DataType>(type) || !reader.Read<UnsignedTinyInt>(flags))
                return false;

            columnView._type = type;
            columnView._isConstant = (flags & Network::ResultFormat::COLUMN_IS_CONSTANT) != 0;

            if (!reader.ReadSpan(columnView._validity, Network::ResultFormat::ValidityBytes(this->_rowCount)))
                return false;

            if (Network::ResultFormat::IsVariableLengthColumn(type)){
                const auto success = DecodeVariableLengthColumn(reader, columnView, this->_rowCount);
                if (!success)
                    return false;
                continue;
            }

            if (!DecodeFixedLengthColumn(reader, columnView, this->_rowCount))
                return false;
        }

        return true;
    }

    void ResultDecoder::PrintHeader(std::ostream& os) const{
        for (const auto& [_name, _type] : this->_columns)
            os << _name << " || ";

        os << '\n';
    }

    void ResultDecoder::PrintBatch(std::ostream& os) const{
        for (auto index = 0; index < this->_rowCount; ++index){
            for (const auto& column : this->_columnViews){
                if ((column._validity[index >> 3] >> (index & 7)) & 1)
                    os << "NULL";
                else
                    ResultDecoder::PrintValue(os, column, index);

                os << " || ";
            }

            os << '\n';
        }
    }
}
