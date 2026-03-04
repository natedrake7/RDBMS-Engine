#include "../../include/DataStorage/Column.h"

#include "../../include/SystemDatabases/SystemCatalog.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../../Server/include/Server.h"
#include "../../include/DataStorage/Table.h"
#include "DataTypes/DataTypes.StaticData.h"
#include "Memory/MiscAllocator.h"

namespace DatabaseEngine::StorageTypes {
     Column::Column(
         const DataTypes::String& columnName,
         const DataType type,
         const row_size_t recordSize,
         const column_index_t index,
         const bool allowNulls
    ){
        this->name = columnName;
        this->header.recordSize = recordSize;
        this->allowNulls = allowNulls;
        this->header.columnType = type;
        this->header.columnIndex = index;
        this->table = nullptr;
        this->isOverflowed = false;
    }

    Column::Column(
        const Headers::sysColumn& header,
        const column_index_t ordinalPosition ,
        const Table* table
    ){
        const auto normalizedType = DataTypes::String::Normalize(header.type);
        const auto strView = normalizedType.ToView();

        this->name = header.name;
        this->allowNulls = false;
        this->header.columnType = ColumnTypesDictionary.Get(&strView);

        const auto size = ColumnTypeSizes.Get(&strView);

        this->header.recordSize = size == 0 ? header.size : size;
        this->header.columnIndex = ordinalPosition;
        this->table = table;
        this->isOverflowed = false;
    }

    Column::Column(const Headers::ColumnHeader& masterDbHeader, const Table* table){
        this->header.id = masterDbHeader.id;
        this->name = masterDbHeader.name;
        this->allowNulls = masterDbHeader.isNullable;
        this->header.columnType = static_cast<DataType>(masterDbHeader.dataType);
        this->header.recordSize = masterDbHeader.recordSize;
        this->header.columnIndex = masterDbHeader.ordinalPosition;
        this->table = table;
        this->isOverflowed = false;
    }

    Column::~Column() = default;

    const DataTypes::String& Column::GetColumnName() const{ return this->name; }

    void Column::SetColumnName(const DataTypes::String &otherName){ this->name = otherName;}

    DataType Column::Type() const { return this->header.columnType; }

    row_size_t Column::Size() const { return this->header.recordSize; }

    bool Column::IsNullable() const { return this->allowNulls; }

    void Column::SetOrdinalPosition(const column_index_t columnIndex) { this->header.columnIndex = columnIndex; }

    column_index_t Column::OrdinalPosition() const { return this->header.columnIndex; }

    const ColumnHeader& Column::GetColumnHeader() const { return this->header; }

    bool Column::isColumnLOB() const { return this->header.recordSize >= LARGE_DATA_OBJECT_SIZE; }

    bool Column::isColumnOverflowed() const{ return this->isOverflowed; }

    Int Column::GetColumnId() const{ return this->header.id; }

    void Column::SetColumnId(const Int columnId){ this->header.id = columnId; }

    void Column::SetIdentityManagerIds(const Int tableId){ this->identityManager.SetHeaderIds(tableId, this->header.id); }

    const Headers::IdentityColumnsHeader & Column::GetIdentity()const { return this->identityManager.GetHeader(); }

    void Column::SetIdentity(const Headers::IdentityColumnsHeader  &identity) { this->identityManager.SetHeader(identity); }

    void Column::SetDefaultValue(const Headers::DefaultValuesHeader &defaultValue){ this->header.defaultValue = defaultValue; }

    const Headers::DefaultValuesHeader & Column::GetDefaultValue() const{ return this->header.defaultValue; }

    void Column::SetIsOverflowed(const bool isOverflow){ this->isOverflowed = isOverflow; }

    BigInt Column::GenerateIdentityValue(const ::Memory::IAllocator* allocator){
        return this->identityManager.Generate(allocator);
    }

    void Column::UpdateMetadata(const ::Memory::IAllocator* allocator)const{
        this->identityManager.UpdateMasterDb(allocator);
    }

    bool Column::HasIdentity() const{
        return this->identityManager.IsValid();
    }
}
