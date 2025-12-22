#pragma once
#include <string>

namespace DatabaseEngine {
    class Database;

    class TemporaryDatabase {
        std::string name;
        std::string path;

        Database* db;

        void ReadConfiguration(const std::string& configPath);
        TemporaryDatabase();
        ~TemporaryDatabase();

        bool Exists()const;
        void ClearTemporaryFiles() const;

        public:
            TemporaryDatabase(TemporaryDatabase const&) = delete;
            void operator=(TemporaryDatabase const&) = delete;
            TemporaryDatabase(TemporaryDatabase&&) = delete;
            void operator=(TemporaryDatabase&&) = delete;

            static TemporaryDatabase &Get();
            void Initialize(const std::string& configPath);

            void Shutdown();
    };
}
