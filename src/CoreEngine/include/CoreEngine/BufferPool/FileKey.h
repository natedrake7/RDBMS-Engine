#pragma once
#include <Systemic/Constants.h>
#include <Systemic/DataTypes/DataTypes.h>

namespace Storage{
    enum class FileType : UnsignedTinyInt{
        Data = 0,
        System = 1
    };

    struct FileKey{
        UnsignedTinyInt _slotIndex;
        FileType type;

        FileKey()
            : _slotIndex(0), type(FileType::Data){}

        explicit FileKey(const UnsignedTinyInt slotIndex, const FileType type)
            : _slotIndex(slotIndex), type(type){}

        static FileKey Create(const UnsignedTinyInt slotIndex, const FileType type){
            return FileKey(slotIndex, type);
        }

        bool operator==(const FileKey& other) const{
            return std::memcmp(this, &other, sizeof(FileKey)) == 0;
        }
    };

    static_assert(std::is_trivially_copyable_v<FileKey>, "FileKey must be trivially copyable");
}

template<>
struct std::hash<Storage::FileKey>{
    size_t operator()(const Storage::FileKey& key) const noexcept{
        return std::hash<Int>()(key._slotIndex) ^ std::hash<Storage::FileType>()(key.type);
    }
};