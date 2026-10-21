#include <CoreEngine/BufferPool/FileManager.h>
#include <filesystem>

namespace Storage{
    FileManager::~FileManager(){
        this->CloseAllFiles();
    }

    void FileManager::OpenFile(
        const buffer_pool_slot_t slotIndex,
        const DataTypes::StringView& pathView,
        const FileOpenMode mode,
        const FileOpenFlags flags
    ){
        auto& entry = this->_table[slotIndex];
        assert(!entry.IsOpen() && "slot already holds an open file");

        if (pathView.Size() >= File::MAX_PATH_SIZE)
            throw std::runtime_error("FileManager:Open: path too long");

        const auto handle = FileIO::Open(pathView, mode, flags);

        std::memcpy(entry._path, pathView.Data(), pathView.Size());
        entry._path[pathView.Size()] = '\0';
        entry._handle.store(handle, std::memory_order_release);
    }

    native_file_t FileManager::GetHandle(const buffer_pool_slot_t slotIndex) const{
        return this->_table[slotIndex]._handle.load(std::memory_order_acquire);
    }

    void FileManager::CloseFile(const buffer_pool_slot_t slotIndex){
        FileIO::Close(this->_table[slotIndex]._handle.exchange(INVALID_NATIVE_FILE, std::memory_order_acq_rel));
    }

    bool FileManager::FileExists(const DataTypes::StringView& fileName){
        char path[DIRECTORY_SIZE];
        std::snprintf(path, sizeof(path), "%.*s", fileName.Size(), fileName.Data());
        return access(path, F_OK) == 0;
    }

    void FileManager::RemoveFile(const DataTypes::StringView& fileName){
        char path[DIRECTORY_SIZE];
        std::snprintf(path, sizeof(path), "%.*s", fileName.Size(), fileName.Data());
        std::filesystem::remove_all(path);
    }

    void FileManager::CloseAllFiles(){
        for (Int i = 0;i < CoreEngine::StorageLayout::MAX_BUFFER_POOL_SLOTS;i++)
            this->CloseFile(i);
    }
};