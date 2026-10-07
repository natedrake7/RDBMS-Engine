#pragma once
#include <CoreEngine/BufferPool/FileKey.h>
#include <Systemic/Constants.h>
#include <Systemic/DataStructures/Dictionary.h>
#include <Systemic/Guards/Mutex.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/String.h>

namespace DataTypes{
    class StringView;
}

namespace Storage{
    constexpr size_t MAX_OPEN_FILES = 2;

    struct File{
        DataTypes::StringView _filename;
        file_descriptor_t _fd;

        explicit File(const DataTypes::StringView& filename, file_descriptor_t fd);
        [[nodiscard]] static Int Read(file_descriptor_t fd, void* data, size_t size, size_t offSet);
        [[nodiscard]] static Int Write(file_descriptor_t fd, const void* data, size_t size, size_t offSet);
        static void Flush(file_descriptor_t fd);
    };

    class FileManager final {
        Dictionary<FileKey, File> fileTable;
        mutable MultiThreading::Mutex tableMutex; // protects pageTable_ and frame insertion

        static constexpr Int DIRECTORY_SIZE = 512;

        public:
            explicit FileManager();
            FileManager(const FileManager& other) = delete;
            ~FileManager();
            void CreateFile_(
                FileKey key,
                const DataTypes::StringView& filename,
                const DataTypes::StringView& extension
            );
            void OpenFile(FileKey key, const DataTypes::StringView& filename);
            [[nodiscard]] file_descriptor_t GetFile(FileKey key);
            void CloseFile(FileKey key);

            [[nodiscard]] static bool FileExists(const DataTypes::StringView& fileName);
            static void RemoveFile(const DataTypes::StringView& fileName);
    };
}