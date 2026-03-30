#pragma once

namespace DatabaseEngine::Messages
{
    static constexpr auto NO_ROWS_TO_INSERT = DataTypes::StringView("No rows to insert");

    static DataTypes::String DUPLICATE_KEY(const ::Memory::IAllocator* allocator, const DataTypes::String& key){
        return DataTypes::String::Concat(allocator, "BTree::CreateDuplicateKeyError: Key: ", key);
    }
}
