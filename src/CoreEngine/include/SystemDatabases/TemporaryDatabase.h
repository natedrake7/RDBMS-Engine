#pragma once
#include <atomic>
#include "../../../Systemic/include/Coercions/Coercions.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine {
    namespace StorageTypes{
        class Table;
    }

    class Database;

    class TemporaryDatabase {
        DataTypes::String name;
        DataTypes::String path;

        Database* _db;
        std::atomic<int> currentOrdinalPosition;

        static std::tuple<DataTypes::String, DataTypes::String> ReadConfiguration(
            const ::Memory::IAllocator* allocator,
            const DataTypes::StringView& configPath
        );
        TemporaryDatabase();
        ~TemporaryDatabase();

        static bool Exists(const DataTypes::StringView& filename);
        static void ClearTemporaryFiles(const ::Memory::IAllocator* allocator, const DataTypes::String& dbName);

        [[nodiscard]] Int GetNextOrdinalPosition();

        public:
            TemporaryDatabase(TemporaryDatabase const&) = delete;
            void operator=(TemporaryDatabase const&) = delete;
            TemporaryDatabase(TemporaryDatabase&&) = delete;
            void operator=(TemporaryDatabase&&) = delete;

            static TemporaryDatabase &Get();
            void Initialize(
                const ::Memory::IAllocator* allocator,
                const DataTypes::StringView& configPath
            );

            [[nodiscard]] StorageTypes::Table* CreateTable();
            [[nodiscard]] StorageTypes::Table* OpenTable(Int tableId) const;

            void Shutdown();
    };
}
