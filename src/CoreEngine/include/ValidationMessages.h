#pragma once

namespace CoreEngine::Messages
{
    static constexpr auto NO_ROWS_TO_INSERT = DataTypes::StringView("No rows to insert");

    static DataTypes::String DUPLICATE_KEY(const ::Memory::IAllocator* allocator, const DataTypes::String& key){
        return DataTypes::String::Concat(allocator, "BTree::CreateDuplicateKeyError: Key: ", key);
    }

    static DataTypes::String COLUMN_SIZE_EXCEEDED(const ::Memory::IAllocator* allocator, const DataTypes::StringView& columnName){
        return DataTypes::String::Concat(allocator, "Column size exceeded: ", columnName);
    }

    static constexpr DataTypes::StringView TABLE_LAYOUT_TOO_LARGE =  "Row cannot fit in a page: reduce key or fixed-size columns";
}
