#pragma once
#include "../../../Systemic/include/Constants.h"
#include "../../../Systemic/include/DataStructures/Dictionary.h"
#include "../../../Systemic/include/Guards/ReadWriteMutex.h"
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
        file_descriptor_t fd;

        explicit File(file_descriptor_t fd);
        [[nodiscard]] Int Read(void* data, size_t size, size_t offSet) const;
        Int Write(const void* data, size_t size, size_t offSet) const;
        void Flush() const;
    };

    class FileManager final {
        Dictionary<FileKey, file_descriptor_t> fileTable;
        mutable MultiThreading::ReadWriteMutex tableMutex; // protects pageTable_ and frame insertion

        static constexpr Int DIRECTORY_SIZE = 512;
        protected:
            file_descriptor_t OpenFile(FileKey key, const DataTypes::StringView& fileName);
            // void RemoveFile();
        
        public:
            explicit FileManager();
            FileManager(const FileManager& other) = delete;
            ~FileManager();
            void CreateFile(
                FileKey key,
                const DataTypes::StringView& fileName,
                const DataTypes::StringView& extension
            );
            [[nodiscard]] File GetFile(FileKey key, const DataTypes::StringView& fileName);
            void CloseFile(FileKey key) const;

            [[nodiscard]] static bool FileExists(const DataTypes::StringView& fileName);
            static void RemoveFile(const DataTypes::StringView& fileName);
    };
}