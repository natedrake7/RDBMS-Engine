#pragma once
#include <string>
#include <vector>

using namespace std;

class HashTable {
    private:
        vector<vector<unsigned char*>> table;
        unsigned int numOfBuckets;

    protected:
        uint32_t HashToBucket(const char* string);
        uint32_t HashToBucket(const uint32_t& hashValue);
    public:
        static uint32_t Hash(const char* stringData);
        static uint32_t Hash(const uint32_t& integerData);
        explicit HashTable(const unsigned int& numOfBuckets);
        virtual ~HashTable();
        uint32_t Insert(const char* input);
        void Insert(const uint32_t& input, const char* inputString);
        const char* GetStringBySecondaryHashTableKey(const uint32_t &primaryHashKey, const uint32_t& secondaryHashKey);

        bool ContainsPrimaryHashTableKey(const char *inputString);
};
