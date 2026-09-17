#pragma once
#include <vector>

#include "../../../Systemic/include/Network/Header.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataStructures/PolymorphicArray.h"

#include "../../../CoreEngine/include/Vectorization/Vectorization.h"

namespace CoreEngine
{
    struct DataVector;
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

        static void PutValidity(
            std::vector<char>* buffer,
            const CoreEngine::DataVector* column,
            Int rowOffset,
            Int rowCount
        );

        static void PutFixedSizeData(
            std::vector<char>* buffer,
            const CoreEngine::DataVector* column,
            Int rowOffset,
            Int rowCount
        );

        template<typename T>
        static void PutVariableSizeData(
            std::vector<char>* buffer,
            const CoreEngine::DataVector* column,
            Int rowOffset,
            Int rowCount,
            const ::Memory::IAllocator* allocator
        );

        public:
        /**
             * Encodes the result schema(column names etc) for the given running query
             * @param buffer stores the raw data to be sent over the socket connection
             * @param requestId the current requestId given by the client, which is an auto increment unsigned integer
             * @param statementOrdinal the ordinal for current statement, since multiple statements can be executed in the same request
             * @param columnNames the names of the columns for the returning schema
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
                Int rowCount,
                const ::Memory::IAllocator* allocator
            );
    };

    template <typename T>
    void ResultEncoder::PutVariableSizeData(
        std::vector<char>* buffer,
        const CoreEngine::DataVector* column,
        const Int rowOffset,
        const Int rowCount,
        const Memory::IAllocator* allocator
    ){
        const auto base = buffer->size();
        buffer->resize(base + (rowCount + 1) * sizeof(UnsignedInt));

        UnsignedInt offset = 0;
        const auto append = [&](const char* data, const Int size){
            buffer->insert(buffer->end(), data, data + size);
            offset += size;
        };

        for (Int i = 0; i < rowCount; i++){
            std::memcpy(buffer->data() + base + (i * sizeof(UnsignedInt)), &offset, sizeof(UnsignedInt));

            const auto index = column->PhysicalIndex(rowOffset + i);

            if (column->GetNullValue(index))
                continue;

            const auto* value = column->SlotAt<T>(index);
            if constexpr (std::is_same_v<T, DataTypes::StringValue>){
                append(value->Data(), value->Size());
            }
            else if constexpr (std::is_same_v<T, DataTypes::JsonBinary>){
                const auto strValue = value->ToStringValue();
                append(strValue.Data(), strValue.Size());
            }
            else if constexpr (std::is_same_v<T, DataTypes::Decimal>){
                const auto strValue = value->ToStringValue(allocator);
                append(strValue.Data(), strValue.Size());
            }
            else
                static_assert(DataTypes::AlwaysFalse<T>, "Unsupported type for variable size data in: ResultEncoder::PutVariableSizeData");
        }

        std::memcpy(buffer->data() + base + rowCount * sizeof(UnsignedInt), &offset, sizeof(UnsignedInt));
    }
}
