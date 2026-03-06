#pragma once
#include "../Database.h"
#include <string>
#include "../../Systemic/include/Errors.h"

namespace DatabaseEngine{
    class VersionDatabase {
    DatabaseHeader header;
        MultiThreading::ReadWriteMutex lastUsedPageMutex;
        MultiThreading::ReadWriteMutex gamPageMutex;
        MultiThreading::ReadWriteMutex pfsPageMutex;

        DataTypes::String filename;
        DataTypes::String systemFilename;

        DataTypes::StringView fileExtension;
        DataTypes::StringView filenameView;
        DataTypes::StringView systemFilenameView;

        Storage::FileKey dataFileKey;
        Storage::FileKey systemFileKey;

        Memory::PersistentAllocator _allocator;

        page_id_t lastUsedPageId;

        void PopulateFilenames(const ::Memory::IAllocator* allocator, const DataTypes::String& dbName);
        void WriteHeaderToFile()const;

        bool AllocateNewExtent(
            page_id_t& newPageId,
            extent_id_t& newExtentId
        );

        Pages::PageView TryGetLastUndoPage(
            const StorageTypes::Table* table,
            row_size_t size
        );

        Pages::PageView CreateUndoPage();
        Pages::PageView GetLastUndoPage(const StorageTypes::Table* table, row_size_t size);

        VersionDatabase();
        ~VersionDatabase();

        static std::tuple<DataTypes::String, DataTypes::String> ReadConfiguration(
            const ::Memory::IAllocator* allocator,
            const DataTypes::StringView& configPath
        );

        [[nodiscard]] bool VersionDatabaseExists()const;

        public:
            VersionDatabase(VersionDatabase const&) = delete;
            void operator=(VersionDatabase const&) = delete;
            VersionDatabase(VersionDatabase&&) = delete;
            void operator=(VersionDatabase&&) = delete;

            static VersionDatabase& Get();

            void Initialize(
                const ExecutionContext& baseContext,
                const DataTypes::StringView& configPath
            );

            Errors::RuntimeStatus InsertRow(
                const Pages::RawRowReference& rowRef,
                StorageTypes::RowVersionPointer& rowPointer,
                const StorageTypes::Table* table
            );
            Pages::RowReference RetrieveRowReference(
                const ::Memory::IAllocator* allocator,
                const Snapshot& snapshot,
                const StorageTypes::RowVersionPointer& rowPointer,
                const StorageTypes::Table* table
            )const;
            [[nodiscard]] std::vector<extent_id_t> GetAllocatedExtents(extent_id_t startingExtentId)const;

            [[nodiscard]] extent_id_t CleanupVersionedData(
                const ::Memory::IAllocator* allocator,
                transaction_id_t transactionId,
                extent_id_t startingExtentId = 0
            )const;
    };
}
