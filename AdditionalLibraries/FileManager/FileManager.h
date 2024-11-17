#pragma once
#include <fstream>
#include <list>
#include <unordered_map>

constexpr size_t MAX_OPEN_FILES = 100;

using namespace std;

typedef struct File {
    string name;
    fstream* filePtr;

    explicit File(const string& filename);
    ~File();
}File;

typedef list<File*>::iterator FileIterator;

class FileManager {
    list<File*> filesList;
    unordered_map<string, FileIterator> cache;
    
    protected:
        void OpenFile(const string& fileName);
        void RemoveFile();
    
    public:
        explicit  FileManager();
        ~FileManager();
        fstream* GetFile(const string& fileName);
        void CloseFile(const string& fileName);
};

