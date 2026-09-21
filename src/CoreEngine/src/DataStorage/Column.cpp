#include "../../include/DataStorage/Column.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../include/DataStorage/Table.h"
#include "DataTypes/DataTypes.StaticData.h"
#include "Memory/PersistentAllocator.h"

namespace CoreEngine::StorageTypes {
     Column::Column(
         const DataTypes::StringView& columnName,
         const DataType type,
         const row_size_t recordSize,
         const column_index_t index,
         const bool allowNulls
    ){
        this->SetColumnName(columnName);
        this->_header.recordSize = recordSize;
        this->_allowNulls = allowNulls;
        this->_header.columnType = type;
        this->_header.columnIndex = index;
        this->_table = nullptr;
        this->_isOverflowed = false;
        this->_isLob = false;
    }

    Column::Column(
        const Headers::sysColumn& header,
        const column_index_t ordinalPosition ,
        const Table* table
    ){
        const auto normalizedType = DataTypes::String::Normalize(header.type, &this->_allocator);
        const auto strView = DataTypes::StringView::ViewOf(normalizedType);

        this->SetColumnName(DataTypes::StringView(header.name));
        this->_allowNulls = false;
        this->_header.columnType = COLUMN_TYPENAMES_TO_ENUMS.Get(&strView);

        const auto size = COLUMN_SIZES_BY_TYPENAME.Get(&strView);

        this->_header.recordSize = size == 0 ? header.size : size;
        this->_header.columnIndex = ordinalPosition;
        this->_table = table;
        this->_isOverflowed = false;
    }

    Column::Column(const Headers::ColumnHeader& masterDbHeader, const Table* table){
        this->_header._id = masterDbHeader.id;
        this->SetColumnName(DataTypes::StringView::ViewOf(masterDbHeader.name));
        this->_allowNulls = masterDbHeader.isNullable;
        this->_header.columnType = static_cast<DataType>(masterDbHeader.dataType);
        this->_header.recordSize = masterDbHeader.recordSize;
        this->_header.columnIndex = masterDbHeader.ordinalPosition;
        this->_table = table;
        this->_isOverflowed = false;
    }

    void Column::Destroy() const{
         this->_allocator.Release();
    }

    Column::~Column(){
        // this->_allocator.Reset();
    }

    const DataTypes::String& Column::GetColumnName() const{ return this->_name; }

    void Column::SetColumnName(const DataTypes::StringView& otherName){ this->_name = DataTypes::String::FromView(otherName, &this->_allocator);}

    DataType Column::Type() const { return this->_header.columnType; }

    row_size_t Column::Size() const { return this->_header.recordSize; }

    bool Column::IsNullable() const { return this->_allowNulls; }

    void Column::SetOrdinalPosition(const column_index_t columnIndex) { this->_header.columnIndex = columnIndex; }

    column_index_t Column::OrdinalPosition() const { return this->_header.columnIndex; }

    const ColumnHeader& Column::GetColumnHeader() const { return this->_header; }

    bool Column::isColumnLOB() const { return this->_isLob; }

    bool Column::isColumnOverflowed() const{ return this->_isOverflowed; }

    Int Column::GetColumnId() const{ return this->_header._id; }

    void Column::SetColumnId(const Int columnId){ this->_header._id = columnId; }

    void Column::SetIdentityManagerIds(const Int tableId){ this->_identityManager.SetHeaderIds(tableId, this->_header._id); }

    const Headers::IdentityColumnsHeader & Column::GetIdentity()const { return this->_identityManager.GetHeader(); }

    void Column::SetIdentity(const Headers::IdentityColumnsHeader  &identity) { this->_identityManager.SetHeader(identity); }

    void Column::SetDefaultValue(const Headers::DefaultValuesHeader &defaultValue){ this->_header.defaultValue = defaultValue; }

    const Headers::DefaultValuesHeader & Column::GetDefaultValue() const{ return this->_header.defaultValue; }

    void Column::SetIsOverflowed(const bool isOverflow){ this->_isOverflowed = isOverflow; }

    void Column::SetIsLob(const bool isLob){
        this->_isLob = isLob;
    }

    BigInt Column::GenerateIdentityValue(const ::Memory::IAllocator* allocator){
        return this->_identityManager.Generate(allocator);
    }

    void Column::UpdateMetadata(const ::Memory::IAllocator* allocator)const{
        this->_identityManager.UpdateMasterDbOnShutdown(allocator);
    }

    bool Column::HasIdentity() const{
        return this->_identityManager.IsValid();
    }
}
