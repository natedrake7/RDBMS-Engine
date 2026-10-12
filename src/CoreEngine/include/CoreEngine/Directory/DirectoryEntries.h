#pragma once

#include <compare>
#include <string_view>

#include "Systemic/DataTypes/DataTypes.h"

namespace CoreEngine{
    class Database;
    namespace StorageTypes{
        class Table;
    }
}

namespace CoreEngine::Directory{

    struct TableKey{
        Int _schemaId;
        DataTypes::StringView _lowerName;

        std::strong_ordering operator<=>(const TableKey&) const = default;
        bool operator==(const TableKey&) const = default;

        explicit TableKey()
            : _schemaId(0){}

        explicit TableKey(const Int schemaId, const DataTypes::StringView lowerName)
            :_schemaId(schemaId), _lowerName(lowerName){}

        TableKey(const TableKey&) = default;
        TableKey& operator=(const TableKey&) = default;

        TableKey(TableKey&&) = default;
        TableKey& operator=(TableKey&&) = default;
    };

    struct TableEntry{
        TableKey _key;
        StorageTypes::Table* _table;

        [[nodiscard]] const TableKey& Key() const{
            return this->_key;
        }

        TableEntry(StorageTypes::Table* table, const TableKey& key)
            : _key(key), _table(table){}
    };

    struct SchemaEntry{
        DataTypes::StringView _lowerName;
        Int _schemaId;

        [[nodiscard]] DataTypes::StringView Key()const{
            return this->_lowerName;
        }

        SchemaEntry(const DataTypes::StringView lowerName, const Int schemaId)
            : _lowerName(lowerName), _schemaId(schemaId){}
    };

    struct DatabaseEntry{
        DataTypes::StringView _lowerName;
        Database* _database;

        [[nodiscard]] DataTypes::StringView Key()const{
            return this->_lowerName;
        }

        DatabaseEntry(const DataTypes::StringView lowerName, Database* database)
            : _lowerName(lowerName), _database(database){}
    };
}
