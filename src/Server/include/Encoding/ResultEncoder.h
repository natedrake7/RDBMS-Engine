#pragma once
#include <vector>

#include "../../../CoreEngine/include/Logger/CrashRecoveryLogger.h"
#include "../../../Systemic/include/Network/Header.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine
{
    struct DataChunk;
    struct OutputSchema;
}

namespace Network{
    class ResultEncoder{
        template<typename T>
        static void Put(std::vector<char>* buffer, const T& value){
            const auto* bytes = reinterpret_cast<const char*>(&value);
            buffer->insert(buffer->end(), bytes, bytes + sizeof(T));
        }

        static void BeginFrame(std::vector<char>* buffer);
        static void EndFrame(
            std::vector<char>* buffer,
            MessageType messageType,
            request_id_t requestId,
            statement_ordinal_t statementOrdinal
        );

        public:

        /**
             * Encodes the result schema(column names etc) for the given running query
             * @param buffer stores the raw data to be sent over the socket connection
             * @param requestId the current requestId given by the client, which is an auto increment unsigned integer
             * @param statementOrdinal the ordinal for current statement, since multiple statements can be executed in the same request
             * @param querySchema the output schema for the current running query
             */
            static void EncodeRowDescription(
                std::vector<char>* buffer,
                request_id_t requestId,
                statement_ordinal_t statementOrdinal,
                const DataStructures::PolymorphicArray<DataTypes::String>& columnNames,
                const CoreEngine::OutputSchema* querySchema
            );

            static void EncodeDataBatch(
                std::vector<char>* buffer,
                request_id_t requestId,
                statement_ordinal_t statementOrdinal,
                const CoreEngine::DataChunk* chunk,
                Int rowOffset,
                Int rowCount
            );
    };
}
