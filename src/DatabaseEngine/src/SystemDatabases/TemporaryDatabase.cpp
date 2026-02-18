#include "../include/SystemDatabases/TemporaryDatabase.h"

#include <fstream>
#include <nlohmann/json.hpp>

#include "Database.h"
#include "Managers/GlobalMemoryManager.h"

namespace DatabaseEngine {
    void TemporaryDatabase::ReadConfiguration(const std::string_view configPath){
        std::ifstream file(configPath.data());

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

        this->name = jsonFile.at("temp_db_name");
        this->path = jsonFile.at("temp_db_path");
    }

    TemporaryDatabase::TemporaryDatabase(){
        this->db = nullptr;
    }

    TemporaryDatabase::~TemporaryDatabase() = default;

    bool TemporaryDatabase::Exists() const{
        return std::filesystem::exists(this->path);
    }

    void TemporaryDatabase::ClearTemporaryFiles() const{
        std::filesystem::remove_all(this->name + "/");
    }

    Int TemporaryDatabase::GetNextOrdinalPosition(){
        return this->currentOrdinalPosition.fetch_add(1, std::memory_order_relaxed);
    }

    TemporaryDatabase& TemporaryDatabase::Get(){
        static TemporaryDatabase instance;
        return instance;
    }

    void TemporaryDatabase::Initialize(const std::string_view configPath){
        this->ReadConfiguration(configPath);

        if (this->Exists())
            this->ClearTemporaryFiles();

        CreateDatabase(this->name);

        this->db = AllocateMiscEntity<Database>(this->name, true);
        // this->db = new Database(this->name, true);
    }

    StorageTypes::Table* TemporaryDatabase::CreateTable(){
        const auto ordinalPosition = this->GetNextOrdinalPosition();

        const std::vector columns = {
            new StorageTypes::Column(
                "col1",
                DataType::Int,
                sizeof(int),
                0,
                false
            )
        };

        return this->db->CreateTable(
            ordinalPosition,
            ordinalPosition,
            columns
        );
    }

    StorageTypes::Table* TemporaryDatabase::OpenTable(const Int tableId) const{
        return this->db->OpenTable(tableId);
    }

    void TemporaryDatabase::Shutdown(){
        DeallocateMiscEntity<Database>(this->db);
        this->db = nullptr;
        // this->ClearTemporaryFiles();
    }
}
