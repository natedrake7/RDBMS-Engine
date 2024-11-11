#include "HashTable.h"

HashTable::HashTable(const unsigned int &numOfBuckets)
{
    this->numOfBuckets = numOfBuckets;
    this->table.resize(this->numOfBuckets);
}

HashTable::~HashTable()
{
    for(const auto& bucket : this->table)
        for(const auto& element : bucket)
            delete[] element;
}

uint32_t HashTable::Insert(const char* input)
{
    const uint32_t hashKey = Hash(input);
    const uint32_t index = this->HashToBucket(hashKey);

    for(const auto& element : this->table[index])
        if(memcmp(element, &hashKey, sizeof(uint32_t)) == 0)
            return hashKey;

    unsigned char* hashValue = new unsigned char[sizeof(uint32_t)];

    memcpy(hashValue, &hashKey, sizeof(uint32_t));
    this->table[index].push_back(hashValue);

    return hashKey;
}

void HashTable::Insert(const uint32_t &input, const char* inputString)
{
    const uint32_t hashKey = Hash(input);
    const uint32_t index = this->HashToBucket(hashKey);

    for(const auto& element : this->table[index])
    {
        const char* value = reinterpret_cast<char*>(element);

        if(input == Hash(value))
            return;
    }

    unsigned char* hashValue = new unsigned char[strlen(inputString) + 1];
    memcpy(hashValue, inputString, strlen(inputString) + 1);

    this->table[index].push_back(hashValue);
}

const char* HashTable::GetStringBySecondaryHashTableKey(const uint32_t &primaryHashKey, const uint32_t& secondaryHashKey)
{
    const uint32_t bucket = HashToBucket(secondaryHashKey);

    for(const auto& element : this->table[bucket])
    {
        const char* value = reinterpret_cast<char*>(element);
        const uint32_t hashKey = Hash(value);

        if(hashKey == primaryHashKey)
            return value;
    }

    return nullptr;
}

bool HashTable::ContainsPrimaryHashTableKey(const char *inputString) {
    const uint32_t hashKey = Hash(inputString);

    const uint32_t index = HashToBucket(hashKey);

    for(const auto& element : this->table[index])
        if(memcmp(element, inputString, sizeof(uint32_t)) == 0)
            return true;

    return false;
}

uint32_t HashTable::Hash(const char* stringData)
{
    const uint8_t* data = reinterpret_cast<const uint8_t*>(stringData);
    uint32_t len = strlen(stringData);
    uint32_t hash = 31;
    uint32_t c1 = 0xcc9e2d51;
    uint32_t c2 = 0x1b873593;

    uint32_t roundedEnd = len / 4 * 4;
    for (uint32_t i = 0; i < roundedEnd; i += 4) {
        uint32_t k = data[i] | (data[i+1] << 8) | (data[i+2] << 16) | (data[i+3] << 24);
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;

        hash ^= k;
        hash = (hash << 13) | (hash >> 19);
        hash = hash * 5 + 0xe6546b64;
    }

    uint32_t k = 0;
    switch (len & 3) {
        case 3: k ^= data[roundedEnd + 2] << 16;
        case 2: k ^= data[roundedEnd + 1] << 8;
        case 1: k ^= data[roundedEnd];
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;
        hash ^= k;
    }

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}

uint32_t HashTable::Hash(const uint32_t &integerData) {
    const uint32_t seed = 0x9747b28c;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t r1 = 15;
    const uint32_t r2 = 13;
    const uint32_t m = 5;
    const uint32_t n = 0xe6546b64;

    uint32_t h = seed;
    uint32_t k = integerData;

    // Mixing step 1
    k *= c1;
    k = (k << r1) | (k >> (32 - r1)); // Rotate left
    k *= c2;

    // Mixing step 2
    h ^= k;
    h = (h << r2) | (h >> (32 - r2)); // Rotate left
    h = h * m + n;

    // Finalization
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

uint32_t HashTable::HashToBucket(const char* string) { return Hash(string) & this->numOfBuckets; }

uint32_t HashTable::HashToBucket(const uint32_t& hashValue) { return hashValue % this->numOfBuckets; }


