#include "../../include/Decoding/ResultDecoder.h"
#include <ostream>

#include "DataTypes/DateTime.h"
#include "DataTypes/Guid.h"
#include "DataTypes/PackedWord.h"
#include "Network/PayloadReader.h"
#include "Network/Header.h"

namespace Client{
    bool ResultDecoder::DecodeVariableLengthColumn(
        Network::PayloadReader& reader,
        ColumnView& columnView,
        const UnsignedInt encodedRows
    ){
        const auto offSetBytes = (static_cast<UnsignedBigInt>(encodedRows + 1)) * sizeof(UnsignedInt);

        if (offSetBytes > MAX_SPAN || !reader.ReadSpan(columnView._offsets, offSetBytes))
            return false;

        std::memcpy(&columnView._blobSize, columnView._offsets + encodedRows * sizeof(UnsignedInt), sizeof(UnsignedInt));

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

        const auto valueBytes = static_cast<UnsignedBigInt>(columnView._width) * encodedRows;
        if (valueBytes > MAX_SPAN || !reader.ReadSpan(columnView._data, valueBytes))
            return false;

        return true;
    }

    void ResultDecoder::PrintValue(std::ostream& os, const ColumnView& columnView, const UnsignedInt row){
        switch (columnView._type){
        case Network::WireType::Bool:{
            auto value = false;
            if (Load(columnView._data, columnView._width, row, value)){
                os << (value ? "TRUE" : "FALSE");
                return;
            }
            break;
        }
        case Network::WireType::TinyInt:{
            TinyInt value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << static_cast<Int>(value); return;
            }
            break;
        }
        case Network::WireType::SmallInt:{
            SmallInt value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << value; return;
            }
            break;
        }
        case Network::WireType::Int:{
            Int value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << value; return;
            }
            break;
        }
        case Network::WireType::BigInt:{
            BigInt value = 0;
            if (Load(columnView._data, columnView._width, row, value)){
                os << value; return;
            }
            break;
        }
        case Network::WireType::DateTime:{
            BigInt timestamp = 0;       // milliseconds since the Unix epoch, UTC
            if (Load(columnView._data, columnView._width, row, timestamp)){
                DataTypes::DateTime(timestamp).Print(os);
                return;
            }
            break;
        }
        case Network::WireType::Guid:{
            if (columnView._width != DataTypes::GUID_SIZE)
                break;

            os << DataTypes::Guid(reinterpret_cast<const object_t*>(columnView._data + row * columnView._width));
            return;
        }
        case Network::WireType::String:
        case Network::WireType::Json:
        case Network::WireType::Decimal:{
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
            UnsignedTinyInt flags = 0;
            UnsignedInt encodedRows = 0;

            if (!reader.Read<Network::WireType>(columnView._type)
                || !reader.Read<UnsignedTinyInt>(flags)
                || !reader.Read<UnsignedInt>(encodedRows)
            ) return false;


            columnView._isConstant = (flags & static_cast<char>(Network::ColumnFlags::Constant)) != 0;

            if (!reader.ReadSpan(columnView._validity, Network::ValidityBytes(encodedRows)))
                return false;

            const auto decoded = (Network::IsVariableLength(columnView._type))
                ? DecodeVariableLengthColumn(reader, columnView, encodedRows)
                : DecodeFixedLengthColumn(reader, columnView, encodedRows);

            if (!decoded)
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
                if (PackedByte::GetBitmapBit(reinterpret_cast<const UnsignedTinyInt*>(column._validity), index))
                    os << "NULL";
                else
                    ResultDecoder::PrintValue(os, column, index);

                os << " || ";
            }

            os << '\n';
        }
    }
}
