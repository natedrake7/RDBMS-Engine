#include "FileManager.h"

File::File(const string& filename)
{
    this->name = filename;
    this->filePtr = new fstream(filename.c_str());
}

File::~File()
{
    this->filePtr->close();
    delete *this->filePtr;
}

FileManager::FileManager() = default;

FileManager::~FileManager() = default;

fstream* FileManager::GetFile(const string& fileName)
{
    const auto& fileIteratorKeyPair = this->cache.find(fileName);
    
    if(fileIteratorKeyPair == this->cache.end())
        this->OpenFile(fileName);

    this->filesList.erase(fileIteratorKeyPair->second);
    this->filesList.push_front(*fileIteratorKeyPair->second);
    this->cache[fileName] = this->filesList.begin();

    return (*fileIteratorKeyPair->second)->filePtr;
}

void FileManager::CloseFile(const string& fileName)
{
    const auto& fileIteratorKeyPair = this->cache.find(fileName);
    if(fileIteratorKeyPair == this->cache.end())
        return;
    
    this->filesList.erase(fileIteratorKeyPair->second);
    this->cache.erase(fileIteratorKeyPair->first);

    delete *fileIteratorKeyPair->second;
}

void FileManager::OpenFile(const string& fileName)
{
    if(this->filesList.size() == MAX_OPEN_FILES)
        this->RemoveFile();

    File* file = new File(fileName);

    if(!file->filePtr->is_open())
    {
        delete file;
        throw runtime_error("File could not be opened");
    }

    this->filesList.push_front(file);
    this->cache[fileName] = this->filesList.begin();
}

void FileManager::RemoveFile()
{
    const auto& fileIterator = this->filesList.end();
        
    this->filesList.erase(fileIterator);
    this->cache.erase((*fileIterator)->name);
        
    delete *fileIterator;
}