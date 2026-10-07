#include <CoreEngine/DataStorage/Column.h>
#include <Systemic/Functions/StringFunctions.h>
#include <CoreEngine/DataStorage/Table.h>
#include <Systemic/DataTypes/DataTypes.StaticData.h>
#include <CoreEngine/Memory/PersistentAllocator.h>

namespace CoreEngine::StorageTypes {
    Column::Column(const Catalog::ColumnHeader& catalogHeader, const Table* table){
        // this->_headers.columnHeader = catalogHeader;
        // this->_name =
        //
        // this->_header._id = catalogHeader.id;
        // this->SetColumnName(DataTypes::StringView::ViewOf(catalogHeader.name));
        // this->_allowNulls = catalogHeader.isNullable;
        // this->_header.columnType = static_cast<DataType>(catalogHeader.dataType);
        // this->_header.recordSize = catalogHeader.recordSize;
        // this->_header.columnIndex = catalogHeader.ordinalPosition;
        // this->_table = table;
    }

    const DataTypes::String& Column::GetColumnName() const{ return this->_name; }

    DataTypes::StringView Column::GetColumnNameView() const{
         return DataTypes::StringView::ViewOf(this->_name);
    }

    void Column::SetColumnName(const DataTypes::StringView& otherName){
        // this->_name = DataTypes::String::FromView(otherName, &this->_allocator);
    }

    DataType Column::Type() const { return static_cast<DataType>(this->_headers.columnHeader.dataType); }

    row_size_t Column::Size() const { return this->_headers.columnHeader.recordSize; }

    bool Column::IsNullable() const { return this->_headers.columnHeader.isNullable; }

    void Column::SetOrdinalPosition(const column_index_t ordinalPosition) { this->_headers.columnHeader.ordinalPosition = ordinalPosition; }

    column_index_t Column::OrdinalPosition() const { return this->_headers.columnHeader.ordinalPosition; }

    Int Column::GetColumnId() const{ return this->_headers.columnHeader.id; }

    void Column::SetColumnId(const Int columnId){ this->_headers.columnHeader.id = columnId; }

    void Column::SetIdentityManagerIds(const Int tableId){ this->_identityManager.SetHeaderIds(tableId, this->_headers.columnHeader.id); }

    const Catalog::IdentityColumnsHeader & Column::GetIdentity()const { return this->_identityManager.GetHeader(); }

    void Column::SetIdentity(const Catalog::IdentityColumnsHeader  &identity) { this->_identityManager.SetHeader(identity); }

    void Column::SetDefaultValue(const Catalog::DefaultValuesHeader &defaultValue){
        this->_headers.defaultValue = defaultValue;
    }

    const Catalog::DefaultValuesHeader & Column::GetDefaultValue() const{ return this->_headers.defaultValue; }

    Int Column::GetIncrement() const{ return this->_identityManager.GetIncrement(); }

    Value Column::GenerateIdentityValue(const ::Memory::IAllocator* allocator){
        // switch (this->_header.columnType){
        //     case DataType::TinyInt:
        //         return Value(this->_identityManager.Generate<TinyInt>(allocator), this->_header.columnIndex);
        //     case DataType::SmallInt:
        //         return Value(this->_identityManager.Generate<SmallInt>(allocator), this->_header.columnIndex);
        //     case DataType::Int:
        //         return Value(this->_identityManager.Generate<Int>(allocator), this->_header.columnIndex);
        //     case DataType::BigInt:
        //         return Value(this->_identityManager.Generate<BigInt>(allocator), this->_header.columnIndex);
        //     default:
        //         throw std::logic_error("Column::GenerateIdentityValue: Invalid column type");
        // }
    }

    void Column::UpdateMetadata(const ::Memory::IAllocator* allocator)const{
        this->_identityManager.UpdateMasterDbOnShutdown(allocator);
    }

    bool Column::HasIdentity() const{
        return this->_identityManager.IsValid();
    }
}
