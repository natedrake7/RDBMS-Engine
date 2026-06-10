#include "../../include/BufferPool/FileManager.h"

#include <fcntl.h>
#include <functional>

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <filesystem>
#include <ranges>
#ifdef WIN32
    #include <io.h>
    #define F_OK 0
    #define access _access
    #define flush _commit
    #define lseek _lseeki64
#else
    #include <unistd.h>
    #define flush fdatasync
    #define lseek ::lseek
#endif

namespace Storage{
    File::File(const DataTypes::StringView& filename, const file_descriptor_t fd)
        : _filename(filename), _fd(fd){}

    Int File::Read(const file_descriptor_t fd, void* data, const size_t size, const size_t offSet){
        lseek(fd, offSet, SEEK_SET);
        const auto result = ::read(fd, data, size);

        if (result < 0){
            perror("File::Read: Failed to read from file");
            throw std::runtime_error("File::Read: Failed to read from file");
        }

        return result;
    }

    Int File::Write(const file_descriptor_t fd, const void* data, const size_t size, const size_t offSet){
        lseek(fd, offSet, SEEK_SET);
        const auto result = ::write(fd, data, size);

        if (result != size)
            throw std::runtime_error("File::Write: Failed to write to file");

        return result;
    }

    void File::Flush(const file_descriptor_t fd){ ::flush(fd); }

    void FileManager::OpenFile(const FileKey key, const DataTypes::StringView& filename){
        char path[DIRECTORY_SIZE];
        std::snprintf(path, sizeof(path), "%.*s", filename.Size(), filename.Data());

        const auto fd = ::open(path, O_RDWR | O_CREAT | O_BINARY, 0644);
        if (fd < 0)
            throw std::runtime_error("FileManager::Open: File could not be opened");

        MultiThreading::WriterGuard lock(&this->tableMutex);
        this->fileTable.Add(key, File(filename, fd));
    }

    FileManager::FileManager() = default;

    FileManager::~FileManager(){
        for(const auto& file : this->fileTable | std::views::values)
            ::close(file._fd);
    }

    void FileManager::CreateFile(
        const FileKey key,
        const DataTypes::StringView& filename,
        const DataTypes::StringView& extension
    ){
        {
            MultiThreading::ReaderGuard lock(&this->tableMutex);
            if (this->fileTable.Contains(key))
                throw std::runtime_error("File already exists");
        }

        char path[DIRECTORY_SIZE];
        std::snprintf(
            path, sizeof(path), "%.*s%.*s",
            filename.Size(), filename.Data(),
            extension.Size(), extension.Data()
        );

        if (access(path, F_OK) == 0)
            throw std::runtime_error("FileManager::CreateFile: File already exists");

        char parentDir[DIRECTORY_SIZE];
        std::strncpy(parentDir, path, sizeof(parentDir));
        parentDir[sizeof(parentDir) - 1] = '\0';

        auto* lastSlash = std::strrchr(parentDir, '/');
        if (lastSlash == nullptr)
            lastSlash = std::strrchr(parentDir, '\\');

        if (lastSlash){
            *lastSlash = '\0';

#ifdef _WIN32
            mkdir(parentDir);
#else
            mkdir(parentDir, 0755);
#endif
        }

        const auto fd = ::open(path, O_RDWR | O_CREAT | O_BINARY, 0644);
        if (fd < 0)
            throw std::runtime_error("FileManager::CreateFile: Database File could not be created");

        MultiThreading::WriterGuard lock(&this->tableMutex);

        if (this->fileTable.Contains(key))
            return;

        this->fileTable.Add(key, File(filename, fd));
    }

    file_descriptor_t FileManager::GetFile(const FileKey key){
        MultiThreading::ReaderGuard lock(&this->tableMutex);
        return this->fileTable.Get(key)._fd;
    }


    void FileManager::CloseFile(const FileKey key){
        MultiThreading::WriterGuard lock(&this->tableMutex);
        const auto* file = &this->fileTable.Get(key);
        ::close(file->_fd);
        this->fileTable.Remove(key);
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

};