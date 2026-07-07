#pragma once
#include "../../../Systemic/include/Constants.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

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