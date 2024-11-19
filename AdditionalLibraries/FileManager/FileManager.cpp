#include "FileManager.h"

File::File(const string& filename)
{
    this->name = filename;
    this->filePtr = new fstream(filename.c_str(), ios::out | ios::in);
    this->lastPageId = -1;
}

File::~File()
{
    this->filePtr->close();
    delete this->filePtr;
}

FileManager::FileManager() = default;

FileManager::~FileManager()
{
    for(const auto& file : this->filesList)
        delete file;
}

void FileManager::CreateFile(const string& fileName, const string& extension)
{
    const auto& fileIteratorKeyPair = this->cache.find(fileName);
    
    if(fileIteratorKeyPair != this->cache.end())
        throw runtime_error("File already exists");
    
    ifstream fileExists(fileName + extension);

    if(fileExists)
        throw runtime_error("Database with name: " + fileName +" already exists");

    fileExists.close();

    ofstream file(fileName + extension);

    if(!file)
        throw runtime_error("Database " + fileName + " could not be created");

    file.close();
}

fstream* FileManager::GetFile(const string& fileName)
{
    const auto& fileIteratorKeyPair = this->cache.find(fileName);
    
    if(fileIteratorKeyPair == this->cache.end())
        this->OpenFile(fileName);
    else
    {
        this->filesList.push_front(*fileIteratorKeyPair->second);
        this->filesList.erase(fileIteratorKeyPair->second);
        this->cache[fileName] = this->filesList.begin();
    }

    return (*this->filesList.begin())->filePtr;
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
    const auto& fileIterator = prev(this->filesList.end());

    this->cache.erase((*fileIterator)->name);

    delete *fileIterator;

    this->filesList.erase(fileIterator);
        
}