#include "../include/Encoding/ResultEncoder.h"

#include <bit>
#include <cassert>

#include "../../../CoreEngine/include/Contexts/OutputSchema.h"
#include "../../../CoreEngine/include/Vectorization/Vectorization.h"
#include "../../../Systemic/include/DataTypes/String.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"
#include "../../../Systemic/include/Network/TypeMapping.h"

static_assert(std::endian::native == std::endian::little);

namespace Network{
    void ResultEncoder::BeginFrame(std::vector<char>* buffer){
        buffer->clear();
        buffer->resize(Header::SIZE);
    }

    void ResultEncoder::EndFrame(
        std::vector<char>* buffer,
        const MessageType messageType,
        const request_id_t requestId,
        const statement_ordinal_t statementOrdinal
    ){
        Header header;
        header._messageType = messageType;
        header._requestId = requestId;
        header._statementOrdinal = statementOrdinal;
        header._payloadLength = buffer->size() - Header::SIZE;
        header.Encode(buffer->data());
    }

    void ResultEncoder::PutValidity(
        std::vector<char>* buffer,
        const CoreEngine::DataVector* column,
        const Int rowOffset,
        const Int rowCount
    ){
        const auto baseSize = buffer->size();
        const auto validityBytesSize = Network::ValidityBytes(rowCount);

        buffer->resize(baseSize + validityBytesSize, 0);

        auto* __restrict__ bufferData = buffer->data() + baseSize;
        const auto* __restrict__ validityWords = column->_validity;
        const auto* __restrict__ validityBytes = reinterpret_cast<const char*>(validityWords);

        switch (column->_kind){
            case CoreEngine::DataVectorKind::Flat:{
                assert(WireBitmap::BitIndex(rowOffset) == 0 && "flat validity std::memcpy requires byte-aligned rowOffset");
                std::memcpy(bufferData, validityBytes + WireBitmap::WordIndex(rowOffset), validityBytesSize);
                break;
            }
            case CoreEngine::DataVectorKind::Constant:
                std::memcpy(bufferData, validityBytes, 1);
                break;
            case CoreEngine::DataVectorKind::Dictionary:{
                assert(column->_selection != nullptr);
                if (column->_selection == nullptr)
                    return;

                const auto* __restrict__ selection = column->_selection + rowOffset;

                const auto isNull = [validityWords](const UnsignedInt p){
                    return EngineBitmap::GetBitmapBit(validityWords, p);
                };

                const auto fullBytes = WireBitmap::WordIndex(rowCount);
                for (UnsignedInt byte = 0; byte < fullBytes; byte++){
                    const auto* rows = selection + (byte << 3);
                    UnsignedTinyInt bits = 0;
                    for (auto k = 0; k < 8; ++k)
                        PackedByte::OrBit(&bits, k, isNull(rows[k]));

                    bufferData[byte] = static_cast<char>(bits);
                }

                // Tail: the last 1..7 rows
                if (const UnsignedTinyInt tail =  WireBitmap::BitIndex(rowCount); tail != 0){
                    const auto* rows = selection + (fullBytes << 3);
                    UnsignedTinyInt bits = 0;
                    for (UnsignedTinyInt k = 0; k < tail; ++k)
                        PackedByte::OrBit(&bits, k, isNull(rows[k]));
                    bufferData[fullBytes] = static_cast<char>(bits);
                }
                break;
            }
        }
    }

    void ResultEncoder::PutFixedSizeData(
        std::vector<char>* buffer,
        const CoreEngine::DataVector* column,
        const Int rowOffset,
        const Int rowCount
    ){
        const auto width = column->_dataEntrySize;
        ResultEncoder::Put<UnsignedSmallInt>(buffer, width);

        if (width == 0 || rowCount == 0)
            return;

        const auto baseSize = buffer->size();
        buffer->resize(baseSize + width * rowCount);

        if (column->IsFlat() || column->IsConstant()){
            std::memcpy(buffer->data() + baseSize, column->_data + rowOffset * width, rowCount * width);
            return;
        }

        auto* __restrict__ bufferData = buffer->data();
        const auto* __restrict__ columnData = column->_data;
        const auto* __restrict__ selection = column->_selection;

        if (selection == nullptr)
            return;

        for (Int i = 0; i < rowCount; i++){
            const auto index = *(selection + rowOffset + i);
            const auto dataOffset = index * width;

            std::memcpy(bufferData + baseSize + (i * width), columnData + dataOffset, width);
        }
    }

    void ResultEncoder::EncodeRowDescription(
        std::vector<char>* buffer,
        const request_id_t requestId,
        const statement_ordinal_t statementOrdinal,
        const DataStructures::PolymorphicArray<DataTypes::String>& columnNames,
        const CoreEngine::OutputSchema* querySchema
    ){
        ResultEncoder::BeginFrame(buffer);
        ResultEncoder::Put<Int>(buffer, columnNames.Size());

        for (auto i = 0; i < columnNames.Size(); i++){
            const auto& name = columnNames[i];
            ResultEncoder::Put<Int>(buffer, name.Size());
            buffer->insert(buffer->end(), name.Data(), name.Data() + name.Size());

            const auto type = Network::ToWire(querySchema->_columns[i]._type);
            ResultEncoder::Put<UnsignedTinyInt>(buffer, static_cast<UnsignedTinyInt>(type));
        }

        ResultEncoder::EndFrame(buffer, MessageType::RowDescription, requestId, statementOrdinal);
    }

    void ResultEncoder::EncodeDataBatch(
        std::vector<char>* buffer,
        const request_id_t requestId,
        const statement_ordinal_t statementOrdinal,
        const CoreEngine::DataChunk* chunk,
        const Int rowOffset,
        const Int rowCount,
        const ::Memory::IAllocator* allocator
    ){
        ResultEncoder::BeginFrame(buffer);
        ResultEncoder::Put<Int>(buffer, rowCount);
        ResultEncoder::Put<Int>(buffer, chunk->_numberOfColumns);

        for (auto i = 0;i < chunk->_numberOfColumns; i++){
            const auto* __restrict__ column = chunk->_columns[i];
            const auto type = Network::ToWire(column->_type);
            const auto isConstant = column->IsConstant();
            const auto encodedOffset = isConstant ? 0 : rowOffset;
            const auto encodedRows = isConstant ? 1 : rowCount;

            if (type == WireType::Invalid)
                throw std::runtime_error("Invalid wire type in ResultEncoder::EncodeDataBatch");

            ResultEncoder::Put<UnsignedTinyInt>(buffer, static_cast<UnsignedTinyInt>(type));

            const auto flags = static_cast<UnsignedTinyInt>(isConstant ? Network::ColumnFlags::Constant : Network::ColumnFlags::None);
            ResultEncoder::Put<UnsignedTinyInt>(buffer, flags);
            ResultEncoder::Put<UnsignedInt>(buffer, encodedRows);
            ResultEncoder::PutValidity(buffer, column, encodedOffset, encodedRows);

            switch (column->_type) {
            case DataType::String:
                ResultEncoder::PutVariableSizeData<DataTypes::StringValue>(buffer, column, encodedOffset, encodedRows, allocator);
                break;
            case DataType::Decimal:
                ResultEncoder::PutVariableSizeData<DataTypes::Decimal>(buffer, column, encodedOffset, encodedRows, allocator);
                break;
            case DataType::Json:
                ResultEncoder::PutVariableSizeData<DataTypes::JsonBinary>(buffer, column, encodedOffset, encodedRows, allocator);
                break;
            case DataType::Bool:
            case DataType::TinyInt:
            case DataType::SmallInt:
            case DataType::Int:
            case DataType::BigInt:
            case DataType::DateTime:
            case DataType::Guid:
            case DataType::Null:
                ResultEncoder::PutFixedSizeData(buffer, column, encodedOffset, encodedRows);
                break;
            default:
                throw std::runtime_error("Unsupported type in ResultEncoder::EncodeDataBatch");
            }
        }

        ResultEncoder::EndFrame(buffer, MessageType::DataBatch, requestId, statementOrdinal);
    }
}
