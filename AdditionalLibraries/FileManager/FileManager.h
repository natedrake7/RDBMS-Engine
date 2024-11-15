#pragma once
#include <fstream>
#include <vector>

using namespace std;

typedef struct File {
    string name;
    fstream file;
}File;

class FileManager {
    vector<File> files;
    size_t maxOpenFiles;

    public:
    explicit  FileManager();
    ~FileManager();
    void openFile(const string& fileName);
    void closeFile();

};

