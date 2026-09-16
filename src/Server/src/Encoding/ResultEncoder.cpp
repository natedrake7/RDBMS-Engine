#include "../include/Encoding/ResultEncoder.h"

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
        const CoreEngine::DataChunk* chunk,
        const CoreEngine::DataVector* column,
        const Int rowOffset,
        const Int rowCount
    ){
        const auto baseSize = buffer->size();
        const auto validityBytes = column->ValidityBytes();

        buffer->resize(baseSize + validityBytes, 0);

        if (column->IsFlat() && chunk->_selection == nullptr && rowOffset == 0){
            std::memcpy(buffer->data() + baseSize, column->_validity, validityBytes);
            return;
        }

        auto* __restrict__ bufferData = buffer->data();

        for (Int i = 0; i < rowCount; i++){
            const auto slotIndex = chunk->RowPhysicalIndex(rowOffset + i);
            const auto columnIndex = column->PhysicalIndex(slotIndex);

            if (column->GetNullValue(columnIndex))
                *(bufferData + baseSize + (i >> 3)) |= static_cast<char>(1u << (i & 7));
        }
    }

    void ResultEncoder::PutFixedSizeData(
        std::vector<char>* buffer,
        const CoreEngine::DataChunk* chunk,
        const CoreEngine::DataVector* column,
        const Int rowOffset,
        const Int rowCount
    ){
        const auto dataSize = column->_dataEntrySize;
        ResultEncoder::Put<UnsignedSmallInt>(buffer, dataSize);

        const auto baseSize = buffer->size();
        buffer->resize(baseSize + dataSize * rowCount);

        if (column->IsFlat() && chunk->_selection == nullptr && rowOffset == 0){
            std::memcpy(buffer->data() + baseSize, column->_data, dataSize * rowCount);
            return;
        }

        auto* __restrict__ bufferData = buffer->data();
        const auto* __restrict__ columnData = column->_data;

        for (Int i = 0; i < rowCount; i++){
            const auto rowIndex = chunk->RowPhysicalIndex(rowOffset + i);
            const auto columnIndex = column->PhysicalIndex(rowIndex);
            const auto dataOffset = columnIndex * dataSize;

            std::memcpy(bufferData + baseSize + (i * dataSize), columnData + dataOffset, dataSize);
        }
    }

    void ResultEncoder::PutVariableSizeData(
        std::vector<char>* buffer,
        const CoreEngine::DataChunk* chunk,
        const CoreEngine::DataVector* column,
        Int rowOffset,
        Int rowCount
    ){
        // const auto
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
}
