#include <CoreEngine/SystemDatabases/TemporaryDatabase.h>

#include <CoreEngine/Database.h>
#include <CoreEngine/Managers/GlobalMemoryManager.h>

namespace CoreEngine {
    TemporaryDatabase::TemporaryDatabase(){
        this->_db = nullptr;
    }

    TemporaryDatabase::~TemporaryDatabase() = default;

    bool TemporaryDatabase::Exists(const DataTypes::StringView& filename){
        return Storage::FileManager::FileExists(filename);
    }

    Int TemporaryDatabase::GetNextOrdinalPosition(){
        return this->currentOrdinalPosition.fetch_add(1, std::memory_order_relaxed);
    }

    TemporaryDatabase& TemporaryDatabase::Get(){
        static TemporaryDatabase instance;
        return instance;
    }

    void TemporaryDatabase::Initialize(const ::Memory::IAllocator* allocator){
        constexpr auto sysDbName = DataTypes::StringView("tempDb");

        // The temporary database never survives a restart. Remove its whole directory: CreateDatabase
        // creates both tempDb.data and tempDb_sys.data, and a leftover of either makes CreateFile throw.
        if (CoreEngine::TemporaryDatabase::Exists(sysDbName))
            Storage::FileManager::RemoveFile(sysDbName);          // std::filesystem::remove_all

        CreateDatabase(allocator, Constants::TEMPORARY_DATABASE_ID, sysDbName);

        this->_db = new Database(
            allocator,
            Constants::TEMPORARY_DATABASE_ID,
            sysDbName
        );
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

        return nullptr;
        // return this->_db->CreateTable(
        //     ordinalPosition,
        //     ordinalPosition
        // );
    }

    StorageTypes::Table* TemporaryDatabase::OpenTable(const Int tableId) const{
        return this->_db->OpenTable(tableId);
    }

    void TemporaryDatabase::Shutdown(){
        this->_db->Destroy();

        delete this->_db;
        this->_db = nullptr;
        // this->ClearTemporaryFiles();
    }
}
