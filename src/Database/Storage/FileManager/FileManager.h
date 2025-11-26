#pragma once

#include "../../../Systemic/DataStructures/Dictionary/Dictionary.h"
#include "../../../Systemic/MultiThreading/ReadWriteMutex/ReadWriteMutex.h"
#include <fstream>
#include <vector>

namespace Storage{
    constexpr size_t MAX_OPEN_FILES = 2;

    typedef struct File {
        std::string name;
        std::fstream* filePtr;

        explicit File(const std::string& filename);
        ~File();
    }File;

    class FileManager final {
        Dictionary<std::string, File*> fileTable;
        mutable MultiThreading::ReadWriteMutex tableMutex; // protects pageTable_ and frame insertion
        
        protected:
            std::fstream* OpenFile(const std::string& fileName);
            // void RemoveFile();
        
        public:
            explicit  FileManager();
            FileManager(const FileManager& other) = delete;
            ~FileManager();
            void CreateFile(const std::string& fileName, const std::string& extension)const;
            std::fstream* GetFile(const std::string& fileName);
            void CloseFile(const std::string& fileName);
    };
}