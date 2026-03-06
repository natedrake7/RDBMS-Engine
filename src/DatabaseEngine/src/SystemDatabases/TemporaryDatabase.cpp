#include "../include/SystemDatabases/TemporaryDatabase.h"

#include <fstream>
#include <nlohmann/json.hpp>

#include "Database.h"
#include "Managers/GlobalMemoryManager.h"

namespace DatabaseEngine {
    std::tuple<DataTypes::String, DataTypes::String> TemporaryDatabase::ReadConfiguration(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& configPath
    ){
        std::ifstream file(configPath.Data());

        if (!file.is_open())
            throw std::runtime_error("System Tables file: " + std::string(configPath) + " could not be opened");

        nlohmann::json jsonFile;

        try {
            file >> jsonFile;
        }
        catch (std::exception &e)
        {
            throw std::runtime_error(e.what());
        }

        DataTypes::String dbName(allocator);
        DataTypes::String dbPath(allocator);

        jsonFile.at("temp_db_name").get_to(dbName);
        jsonFile.at("temp_db_path").get_to(dbPath);

        return std::make_tuple(std::move(dbName), std::move(dbPath));
    }

    TemporaryDatabase::TemporaryDatabase(){
        this->_db = nullptr;
    }

    TemporaryDatabase::~TemporaryDatabase() = default;

    bool TemporaryDatabase::Exists(const DataTypes::StringView& filename){
        return Storage::FileManager::FileExists(filename);
    }

    void TemporaryDatabase::ClearTemporaryFiles() const{
        // std::filesystem::remove_all(this->name + "/");
    }

    Int TemporaryDatabase::GetNextOrdinalPosition(){
        return this->currentOrdinalPosition.fetch_add(1, std::memory_order_relaxed);
    }

    TemporaryDatabase& TemporaryDatabase::Get(){
        static TemporaryDatabase instance;
        return instance;
    }

    void TemporaryDatabase::Initialize(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& configPath
    ){
        const auto [dbName, dbPath] = this->ReadConfiguration(allocator, configPath);

        if (this->Exists(dbName.ToView()))
            this->ClearTemporaryFiles();

        CreateDatabase(TEMPORARY_DATABASE_ID, dbName);

        this->_db = new Database(dbName, true);
    }

    StorageTypes::Table* TemporaryDatabase::CreateTable(){
        const auto ordinalPosition = this->GetNextOrdinalPosition();

        // const std::vector columns = {
        //     new StorageTypes::Column(
        //         "col1",
        //         DataType::Int,
        //         sizeof(int),
        //         0,
        //         false
        //     )
        // };

        return this->_db->CreateTable(
            ordinalPosition,
            ordinalPosition
        );
    }

    StorageTypes::Table* TemporaryDatabase::OpenTable(const Int tableId) const{
        return this->_db->OpenTable(tableId);
    }

    void TemporaryDatabase::Shutdown(){
        delete this->_db;
        this->_db = nullptr;
        // this->ClearTemporaryFiles();
    }
}
