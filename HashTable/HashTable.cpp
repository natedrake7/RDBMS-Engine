#include "HashTable.h"

HashTable::HashTable(const unsigned int &numOfBuckets)
{
    this->numOfBuckets = numOfBuckets;
    this->table.resize(this->numOfBuckets);
}

uint64_t HashTable::Insert(const char* input)
{
    const uint64_t hashKey = Hash(input);
    const uint64_t index = this->HashToBucket(hashKey);
    const size_t inputSize = strlen(input) + 1;

    for(const auto& element : this->table[index])
        if(memcmp(element, input, inputSize) == 0)
            return hashKey;

    unsigned char* inputInBytes = new unsigned char[inputSize];

    memcpy(inputInBytes, input, inputSize);
    this->table[index].push_back(inputInBytes);

    return hashKey;
}

HashTable::~HashTable()
{
    for(const auto& bucket : this->table)
        for(const auto& element : bucket)
            delete[] element;
}


const char* HashTable::GetStringByHashKey(const uint64_t& hashKey)
{
    const uint64_t index = HashToBucket(hashKey);

    for(const auto& element : this->table[index])
    {
        char* value = reinterpret_cast<char*>(element);

        if(Hash(value) == hashKey)
            return value;
    }
    return nullptr;
}

uint64_t HashTable::Hash(const char* stringData) {
    const uint8_t* data = reinterpret_cast<const uint8_t*>(stringData);
    uint64_t len = strlen(stringData);
    uint64_t hash = 31;
    uint64_t c1 = 0x87c37b91114253d5ULL;
    uint64_t c2 = 0x4cf5ad432745937fULL;

    uint64_t roundedEnd = len / 8 * 8;  // Process in 8-byte chunks for 64-bit version
    for (uint64_t i = 0; i < roundedEnd; i += 8) {
        uint64_t k = static_cast<uint64_t>(data[i]) |
                     (static_cast<uint64_t>(data[i + 1]) << 8) |
                     (static_cast<uint64_t>(data[i + 2]) << 16) |
                     (static_cast<uint64_t>(data[i + 3]) << 24) |
                     (static_cast<uint64_t>(data[i + 4]) << 32) |
                     (static_cast<uint64_t>(data[i + 5]) << 40) |
                     (static_cast<uint64_t>(data[i + 6]) << 48) |
                     (static_cast<uint64_t>(data[i + 7]) << 56);

        k *= c1;
        k = (k << 31) | (k >> 33);  // ROTL 31
        k *= c2;

        hash ^= k;
        hash = (hash << 27) | (hash >> 37);  // ROTL 27
        hash = hash * 5 + 0x52dce729;
    }

    uint64_t k = 0;
    switch (len & 7) {  // Handle remaining bytes if any
        case 7: k ^= static_cast<uint64_t>(data[roundedEnd + 6]) << 48;
        case 6: k ^= static_cast<uint64_t>(data[roundedEnd + 5]) << 40;
        case 5: k ^= static_cast<uint64_t>(data[roundedEnd + 4]) << 32;
        case 4: k ^= static_cast<uint64_t>(data[roundedEnd + 3]) << 24;
        case 3: k ^= static_cast<uint64_t>(data[roundedEnd + 2]) << 16;
        case 2: k ^= static_cast<uint64_t>(data[roundedEnd + 1]) << 8;
        case 1: k ^= static_cast<uint64_t>(data[roundedEnd]);
            k *= c1;
            k = (k << 31) | (k >> 33);  // ROTL 31
            k *= c2;
            hash ^= k;
    }

    // Finalization mix - force all bits of a hash block to avalanche
    hash ^= len;
    hash ^= (hash >> 33);
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= (hash >> 33);
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= (hash >> 33);

    return hash;
}

uint64_t HashTable::HashToBucket(const char* string) { return Hash(string) & this->numOfBuckets; }

uint64_t HashTable::HashToBucket(const uint64_t& hashValue) { return hashValue % this->numOfBuckets; }


