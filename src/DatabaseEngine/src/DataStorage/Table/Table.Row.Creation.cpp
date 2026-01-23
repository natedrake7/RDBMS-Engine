#include <cmath>

#include "../../../include/DataStorage/Table.h"
#include "DataStorage/InsertPayload.h"


namespace DatabaseEngine::StorageTypes{
    Errors::RuntimeStatus Table::CreateInsertPayload(
        transaction_id_t transactionId,
        const std::vector<Value> &inputData
    ){
        auto rowHeader = RowHeader();
        auto payload = InsertPayload();
        //calculate header size and offset
        Int dataSizesOffset = Constants::ROW_VERSION_HEADER_SIZE
            + 3 * static_cast<Int>(std::ceil(this->columns.size() / 8));

        //TODO figure out how to avoid unused bytes
        auto dataOffSet = dataSizesOffset + this->columns.size() * sizeof(block_size_t);

        Errors::RuntimeStatus status;
        for (const auto& value : inputData){
            const auto& index = value.GetColumnIndex();
            const auto& column = this->columns.at(index);

            //ignore auto-computed columns even if specified
            if (column->HasIdentity()){
                auto identityValue = column->GenerateIdentityValue();
                payload.SetData(&identityValue, column->GetColumnSize());
                continue;
            }

            if (value.IsNull()){
                rowHeader.nullBitMap.Set(index, true);
                continue;
            }

            auto result = static_cast<block_size_t>(payload.SetData(value, column, status));
            if (!status.IsOk())
                return status;

            payload.SetData(&result, sizeof(block_size_t), dataSizesOffset);
            dataSizesOffset += sizeof(block_size_t);
        }




        return Errors::RuntimeStatus();
    }
}
