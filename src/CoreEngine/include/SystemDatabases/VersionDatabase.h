#pragma once
#include "../Database.h"
#include "../../Systemic/include/Errors.h"

namespace CoreEngine{
    class VersionDatabase {
        MultiThreading::ReadWriteMutex lastUsedPageMutex;
        MultiThreading::ReadWriteMutex gamPageMutex;
        MultiThreading::ReadWriteMutex pfsPageMutex;

        Memory::PersistentAllocator _allocator;

        DataTypes::String filename;
        DataTypes::String systemFilename;

        DataTypes::StringView fileExtension;
        DataTypes::StringView filenameView;
        DataTypes::StringView systemFilenameView;

        DatabaseHeader header;

        Storage::FileKey dataFileKey;
        Storage::FileKey systemFileKey;

        page_id_t lastUsedPageId;

        std::atomic<Int> numberOfPendingVersions;

        static constexpr Int PENDING_VERSIONS_THRESHOLD = 1000;

        void PopulateFilenames(const ::Memory::IAllocator* allocator, const DataTypes::String& dbName);
        void WriteHeaderToFile()const;

        bool AllocateNewExtent(
            const ::Memory::IAllocator* allocator,
            page_id_t& newPageId,
            extent_id_t& newExtentId
        );

        Pages::PageView TryGetLastUndoPage(
            const StorageTypes::Table* table,
            row_size_t size
        );

        Pages::PageView CreateUndoPage(const ::Memory::IAllocator* allocator);
        Pages::PageView GetLastUndoPage(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::Table* table,
            row_size_t size
        );

        VersionDatabase();
        ~VersionDatabase();

        static std::tuple<DataTypes::String, DataTypes::String> ReadConfiguration(
            const ::Memory::IAllocator* allocator,
            const DataTypes::StringView& configPath
        );

        [[nodiscard]] static bool VersionDatabaseExists(const DataTypes::StringView& path);
        void CreateKeys();

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
                const ::Memory::IAllocator* allocator,
                const Pages::RawRowReference& rowRef,
                StorageTypes::RowVersionPointer& rowPointer,
                const StorageTypes::Table* table
            );
            Pages::RowView* RetrieveRowReference(
                const ::Memory::IAllocator* allocator,
                const Snapshot& snapshot,
                const StorageTypes::RowVersionPointer& rowPointer,
                const StorageTypes::Table* table
            )const;
            [[nodiscard]] std::vector<extent_id_t> GetAllocatedExtents(extent_id_t startingExtentId)const;

            [[nodiscard]] extent_id_t CleanupVersionedData(
                transaction_id_t transactionId,
                extent_id_t startingExtentId = 0
            )const;
            [[nodiscard]] bool HasPendingVersions()const;
    };
}
