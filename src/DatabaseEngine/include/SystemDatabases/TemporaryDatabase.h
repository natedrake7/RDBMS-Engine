#pragma once
#include <atomic>
#include <string>

#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace DatabaseEngine {
    namespace StorageTypes{
        class Table;
    }

    class Database;

    class TemporaryDatabase {
        std::string name;
        std::string path;

        Database* db;
        std::atomic<int> currentOrdinalPosition;

        void ReadConfiguration(const std::string& configPath);
        TemporaryDatabase();
        ~TemporaryDatabase();

        bool Exists()const;
        void ClearTemporaryFiles() const;

        [[nodiscard]] Int GetNextOrdinalPosition();

        public:
            TemporaryDatabase(TemporaryDatabase const&) = delete;
            void operator=(TemporaryDatabase const&) = delete;
            TemporaryDatabase(TemporaryDatabase&&) = delete;
            void operator=(TemporaryDatabase&&) = delete;

            static TemporaryDatabase &Get();
            void Initialize(const std::string& configPath);

            [[nodiscard]] StorageTypes::Table* CreateTable();
            [[nodiscard]] StorageTypes::Table* OpenTable(Int tableId) const;

            void Shutdown();
    };
}
