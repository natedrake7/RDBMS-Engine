#pragma once
#include "../../../Systemic/include/Constants.h"
#include "../../../Systemic/include/DataStructures/Dictionary.h"
#include "../../../Systemic/include/Guards/Mutex.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/DataTypes/String.h"

namespace DataTypes{
    class StringView;
}
namespace Storage{
    enum class FileType : UnsignedTinyInt{
        Data = 0,
        System = 1
    };

    struct FileKey{
        Int databaseId;
        FileType type;

        bool operator==(const FileKey& other) const{
            return this->databaseId == other.databaseId
                && this->type == other.type;
        }

        FileKey()
            : databaseId(INVALID_DATABASE_ID), type(FileType::Data){}

        FileKey(const Int databaseId, const FileType type)
            : databaseId(databaseId), type(type){}

        static FileKey Create(const Int databaseId, const FileType type){
            return FileKey(databaseId, type);
        }
    };
}

template<>
struct std::hash<Storage::FileKey>{
    size_t operator()(const Storage::FileKey& key) const noexcept{
        return std::hash<Int>()(key.databaseId) ^ std::hash<Storage::FileType>()(key.type);
    }
};

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
            void CreateFile(
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