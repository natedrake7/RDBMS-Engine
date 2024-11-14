#pragma once
#include <string>
#include <vector>

using namespace std;

class HashTable {
    private:
        vector<vector<unsigned char*>> table;
        unsigned int numOfBuckets;

    protected:
        uint64_t HashToBucket(const char* string);
        uint64_t HashToBucket(const uint64_t& hashValue);
    public:
        static uint64_t Hash(const char* stringData);
    
        explicit HashTable(const unsigned int& numOfBuckets);
        virtual ~HashTable();

        uint64_t Insert(const char* input);

        const char* GetStringByHashKey(const uint64_t& hashKey);
};
