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
    File::File(const file_descriptor_t fd)
        : fd(fd){}

    Int File::Read(void* data, const size_t size, const size_t offSet) const{
        lseek(this->fd, size, SEEK_SET);
        const auto result = ::read(this->fd + offSet, data, size);

        if (result < 0)
            throw std::runtime_error("File::Read: Failed to read from file");

        return result;
    }

    Int File::Write(const void* data, const size_t size, const size_t offSet) const{
        lseek(this->fd, size, SEEK_SET);
        const auto result = ::write(this->fd + offSet, data, size);

        if (result != size)
            throw std::runtime_error("File::Write: Failed to write to file");

        return result;
    }

    void File::Flush() const{ ::flush(this->fd); }

    file_descriptor_t FileManager::OpenFile(const FileKey key, const DataTypes::StringView& fileName)
    {
        const auto fd = ::open(fileName.Data(), O_RDWR | O_CREAT | O_BINARY, 0644);
        if (fd < 0)
            throw std::runtime_error("FileManager::Open: File could not be opened");

        MultiThreading::WriterGuard lock(&this->tableMutex);
        this->fileTable.Add(key, fd);
        return fd;
    }

    FileManager::FileManager() = default;

    FileManager::~FileManager(){
        for(const auto file : this->fileTable | std::views::values)
            ::close(file);
    }

    void FileManager::CreateFile(
        const FileKey key,
        const DataTypes::StringView& fileName,
        const DataTypes::StringView& extension
    ){
        {
            MultiThreading::ReaderGuard lock(&this->tableMutex);
            if (this->fileTable.Contains(key))
                throw std::runtime_error("File already exists");
        }

        char path[DIRECTORY_SIZE];
        std::snprintf(path, sizeof(path), "%s%s", fileName.Data(), extension.Data());

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

        const auto fd = ::open(path, O_WRONLY | O_CREAT | O_BINARY, 0644);
        if (fd < 0)
            throw std::runtime_error("FileManager::CreateFile: Database File could not be created");

        MultiThreading::WriterGuard lock(&this->tableMutex);

        if (this->fileTable.Contains(key))
            return;

        this->fileTable.Add(key, fd);
    }

    File FileManager::GetFile(
        const FileKey key,
        const DataTypes::StringView& fileName
    ){
        {
            MultiThreading::ReaderGuard lock(&this->tableMutex);

            Int fd = 0;
            if (this->fileTable.TryGetValue(key, fd))
                return File(fd);
        }

        return File(this->OpenFile(key, fileName));
    }

    void FileManager::CloseFile(const FileKey key) const{
        MultiThreading::WriterGuard lock(&this->tableMutex);

        file_descriptor_t fd = 0;
        if (!this->fileTable.TryGetValue(key, fd))
            return;

        ::close(fd);
    }

    bool FileManager::FileExists(const DataTypes::StringView& fileName){
        return access(fileName.Data(), F_OK) == 0;
    }
};