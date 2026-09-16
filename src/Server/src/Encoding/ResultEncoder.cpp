#include "../include/Encoding/ResultEncoder.h"

#include "../../../CoreEngine/include/Contexts/OutputSchema.h"

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
        buffer->insert(buffer->begin(), reinterpret_cast<const char*>(&header), reinterpret_cast<const char*>(&header) + Header::SIZE);
    }

    void ResultEncoder::EncodeRowDescription(
        std::vector<char>* buffer,
        request_id_t requestId,
        statement_ordinal_t statementOrdinal,
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
