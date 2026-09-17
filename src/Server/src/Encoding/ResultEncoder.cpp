#include "../include/Encoding/ResultEncoder.h"

#include "ResultFormat.h"
#include "../../../CoreEngine/include/Contexts/OutputSchema.h"
#include "../../../CoreEngine/include/Vectorization/Vectorization.h"
#include "../../../Systemic/include/DataTypes/String.h"
#include "../../../Systemic/include/DataStructures/PolymorphicArray.h"

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
        const auto validityBytes = ResultFormat::ValidityBytes(rowCount);

        buffer->resize(baseSize + validityBytes, 0);

        auto* __restrict__ bufferData = buffer->data() + baseSize;

        if (column->IsFlat() || column->IsConstant()){
            std::memcpy(bufferData, column->_validity, validityBytes);
            return;
        }

        const auto* __restrict__ selection = column->_selection;

        if (selection == nullptr)
            return;

        for (Int i = 0; i < rowCount; i++){
            const auto index = *(selection + rowOffset + i);
            if (column->GetNullValue(index))
                *(bufferData + (i >> 3)) |= static_cast<char>(1u << (i & 7));
        }
    }

    void ResultEncoder::PutFixedSizeData(
        std::vector<char>* buffer,
        const CoreEngine::DataVector* column,
        const Int rowOffset,
        const Int rowCount
    ){
        const auto dataSize = column->_dataEntrySize;
        ResultEncoder::Put<UnsignedSmallInt>(buffer, dataSize);

        const auto baseSize = buffer->size();
        buffer->resize(baseSize + dataSize * rowCount);

        if (column->IsFlat() || column->IsConstant()){
            std::memcpy(buffer->data() + baseSize, column->_data + rowOffset * dataSize, rowCount * dataSize);
            return;
        }

        auto* __restrict__ bufferData = buffer->data();
        const auto* __restrict__ columnData = column->_data;
        const auto* __restrict__ selection = column->_selection;

        if (selection == nullptr)
            return;

        for (Int i = 0; i < rowCount; i++){
            const auto index = *(selection + rowOffset + i);
            const auto dataOffset = index * dataSize;

            std::memcpy(bufferData + baseSize + (i * dataSize), columnData + dataOffset, dataSize);
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

        for (auto i = 0;i < columnNames.Size(); i++){
            const auto& name = columnNames[i];
            ResultEncoder::Put<Int>(buffer, name.Size());
            buffer->insert(buffer->end(), name.Data(), name.Data() + name.Size());

            const auto type = querySchema->_columns[i]._type;
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
            const auto type = column->_type;
            const auto isConstant = column->IsConstant();

            const auto encodedOffset = isConstant ? 0 : rowOffset;
            const auto encodedRows = isConstant ? 1 : rowCount;

            ResultEncoder::Put<UnsignedTinyInt>(buffer, static_cast<UnsignedTinyInt>(type));
            ResultEncoder::Put<UnsignedTinyInt>(buffer, isConstant ? ResultFormat::COLUMN_IS_CONSTANT : 0);
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
                ResultEncoder::PutFixedSizeData(buffer, column, encodedOffset, encodedRows);
                break;
            default:
                throw std::runtime_error("Unsupported type in ResultEncoder::EncodeDataBatch");
            }
        }

        ResultEncoder::EndFrame(buffer, MessageType::DataBatch, requestId, statementOrdinal);
    }
}
