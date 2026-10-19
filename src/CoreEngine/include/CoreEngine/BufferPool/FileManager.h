#pragma once
#include <Systemic/DataStructures/Dictionary.h>
#include <Systemic/Guards/Mutex.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/String.h>

#include "File.h"
#include "StorageLayout.h"

namespace DataTypes{
    class StringView;
}

namespace Storage{
    class FileManager final {
        File _table[CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS];

        inline static constexpr Int DIRECTORY_SIZE = 512;

        public:
            FileManager() = delete;

            FileManager(const FileManager& other) = delete;
            FileManager& operator=(const FileManager& other) = delete;
            FileManager(FileManager&& other) = delete;
            FileManager& operator=(FileManager&& other) = delete;

            ~FileManager();
            void OpenFile(
                buffer_pool_slot_t slotIndex,
                const DataTypes::StringView& pathView,
                FileOpenMode mode,
                FileOpenFlags flags
            );
            [[nodiscard]] native_file_t GetHandle(buffer_pool_slot_t slotIndex) const;
            void CloseFile(buffer_pool_slot_t slotIndex);

            [[nodiscard]] static bool FileExists(const DataTypes::StringView& fileName);
            static void RemoveFile(const DataTypes::StringView& fileName);

            void CloseAllFiles();
    };
}