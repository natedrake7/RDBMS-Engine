#include "Column.h"
#include "../../Systemic/Functions/StringFunctions.h"
#include "../../Server/Server.h"
#include "../Table/Table.h"

namespace DatabaseEngine::StorageTypes {
    

     Column::Column(const std::string& columnName, const DataType& type, const row_size_t&  recordSize, const column_index_t& index, const bool& allowNulls)
    {
        this->name = columnName;
        this->header.recordSize = recordSize;
        this->allowNulls = allowNulls;
        this->header.columnType = type;
        this->header.columnIndex = index;
        this->table = nullptr;
        this->isOverflowed = false;
    }

    Column::Column(const Headers::sysColumn& header, const column_index_t& tablePos , const Table* table)
    {
        const auto normalizedType = Functions::String::NormalizeString(header.type);

        this->name = header.name;
        this->allowNulls = false;
        this->header.columnType = ColumnTypesDictionary.Get(normalizedType);

        const auto size = ColumnTypeSizes.Get(normalizedType);

        this->header.recordSize = size == 0 ? header.size : size;
        this->header.columnIndex = tablePos;
        this->table = table;
        this->isOverflowed = false;
    }

    Column::Column(const Headers::ColumnHeader& masterDbHeader, const Table* table)
    {
        this->header.id = masterDbHeader.id;
        this->name = masterDbHeader.name;
        this->allowNulls = masterDbHeader.isNullable;
        this->header.columnType = static_cast<Constants::DataType>(masterDbHeader.dataType);
        this->header.recordSize = masterDbHeader.recordSize;
        this->header.columnIndex = masterDbHeader.ordinalPosition;
        this->table = table;
        this->isOverflowed = false;
    }

    Column::~Column() = default;

    const string& Column::GetColumnName() const{ return this->name; }

    void Column::SetColumnName(const std::string &name){
        this->name = name;
    }

    const DataType& Column::GetColumnType() const { return this->header.columnType; }

    const row_size_t& Column::GetColumnSize() const { return this->header.recordSize; }

    bool Column::IsColumnNullable() const { return this->table->IsColumnNullable(this->header.columnIndex); }

    const bool& Column::GetAllowNulls() const { return this->allowNulls; }

    void Column::SetColumnIndex(const column_index_t& columnIndex) { this->header.columnIndex = columnIndex; }

    const column_index_t& Column::GetColumnIndex() const { return this->header.columnIndex; }

    const ColumnHeader& Column::GetColumnHeader() const { return this->header; }

    bool Column::isColumnLOB() const { return this->header.recordSize >= LARGE_DATA_OBJECT_SIZE; }

    bool Column::isColumnOverflowed() const{ return this->isOverflowed; }

    const int32_t& Column::GetColumnId() const{ return this->header.id; }

    void Column::SetColumnId(const int32_t &columnId){ this->header.id = columnId; }

    void Column::SetIdentityManagerIds(const int32_t &tableId){
        this->identityManager.SetHeaderIds(tableId, this->header.id);
    }

    const Headers::IdentityColumnsHeader & Column::GetIdentity()const { return this->identityManager.GetHeader(); }

    void Column::SetIdentity(const Headers::IdentityColumnsHeader  &identity) { this->identityManager.SetHeader(identity); }

    void Column::SetDefaultValue(const Headers::DefaultValuesHeader &defaultValue){ this->header.defaultValue = defaultValue; }

    const Headers::DefaultValuesHeader & Column::GetDefaultValue() const{ return this->header.defaultValue; }

    void Column::SetIsOverflowed(const bool & isOverflow){ this->isOverflowed = isOverflow; }

    void Column::SetColumnStatistics(const Headers::ColumnStatistics &stats){ this-> statistics = stats;}

    void Column::SetHistograms(std::vector<Headers::ColumnHistograms> &otherHistograms) {
        this->histograms = std::move(otherHistograms);
    }

//compute distinct count too
    void Column::UpdateColumnStatistics(const Row *row){
        const auto& value = row->GetColumnByIndex(this->header.columnIndex);

        if (value.IsNull()) {
            this->statistics.nullCount++;
            return;
        }

        this->statistics.nullCount = 1;
        this->statistics.distinctCount = 1;

        if ((value < this->statistics.min).GetBool())
            this->statistics.min = value;

        if ((value > this->statistics.max).GetBool())
            this->statistics.max = value;

        Server::ServerInstance::Get().UpdateColumnStatisticsById(
            this->header.id,
            this->statistics.distinctCount,
            this->statistics.nullCount,
            this->statistics.min,
            this->statistics.max
        );
    }

    bool Column::GenerateIdentityValue(int64_t& value){
        return this->identityManager.TryGenerate(value);
    }

    void Column::UpdateMetadata()const{
        this->identityManager.UpdateMasterDb();
    }

    bool Column::HasIdentity() const{
        return this->identityManager.IsValid();
    }
}