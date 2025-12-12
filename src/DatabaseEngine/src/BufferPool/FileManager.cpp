#include "../../include/BufferPool/FileManager.h"

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <filesystem>
#include <ranges>
namespace fs = std::filesystem;

namespace Storage{

    File::File(const std::string& filename)
    {
        this->name = filename;
        this->filePtr = new std::fstream(filename.c_str(), std::ios::out | std::ios::in | std::ios::binary);
    }

    File::~File()
    {
        this->filePtr->flush();
        this->filePtr->close();

        delete this->filePtr;
        filePtr = nullptr;
    }

    FileManager::FileManager() = default;

    FileManager::~FileManager()
    {
        for(const auto &file : this->fileTable | std::views::values)
            delete file;
    }

    void FileManager::CreateFile(const std::string& fileName, const std::string& extension)const
    {
        {
            MultiThreading::ReaderGuard lock(&this->tableMutex);

            if (this->fileTable.Contains(fileName))
                throw std::runtime_error("File already exists");
        }

        std::ifstream fileExists(fileName + extension);

        if(fileExists)
            throw std::runtime_error("Database with name: " + fileName +" already exists");

        fileExists.close();

        auto fullPath = fs::path(fileName + extension);

        auto parentDir = fullPath.parent_path();

        if(!parentDir.empty() && !fs::exists(parentDir))
          fs::create_directories(parentDir);

        std::ofstream file(fileName + extension);

        if(!file)
            throw std::runtime_error("Database " + fileName + " could not be created");

        file.close();
    }

    std::fstream* FileManager::GetFile(const std::string& fileName)
    {
        {
            MultiThreading::ReaderGuard lock(&this->tableMutex);

            File* file = nullptr;
            if (this->fileTable.TryGetValue(fileName, file))
                return file->filePtr;
        }

        return this->OpenFile(fileName);
    }

    void FileManager::CloseFile(const std::string& fileName)
    {
        MultiThreading::WriterGuard lock(&this->tableMutex);

        File* file = nullptr;
        if (!this->fileTable.TryGetValue(fileName, file))
            return;

        delete file;
        this->fileTable.Remove(fileName);
    }

    std::fstream* FileManager::OpenFile(const std::string& fileName)
    {
        // if(this->filesList.size() == MAX_OPEN_FILES)
        //     this->RemoveFile();

        auto* file = new File(fileName);

        if(!file->filePtr->is_open())
        {
            delete file;
            throw std::runtime_error("File could not be opened");
        }

        MultiThreading::WriterGuard lock(&this->tableMutex);

        this->fileTable.Add(fileName, file);

        return file->filePtr;
    }

    // void FileManager::RemoveFile()
    // {
    //     const auto& fileIterator = prev(this->filesList.end());
    //
    //     this->cache.erase((*fileIterator)->name);
    //
    //     delete *fileIterator;
    //
    //     this->filesList.erase(fileIterator);
    // }
};