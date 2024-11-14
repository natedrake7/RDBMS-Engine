#ifndef ROW_H
#define ROW_H
#include "../Table/Table.h"
#include "../Block/Block.h"

class Block;
class Table;
class Database;

using namespace std;

class Row {
    private:
        vector<Block*> data;
        size_t* rowSize;
        size_t* maxRowSize;
        const Table* table;
        bool isOriginalRow;

    public:
        explicit Row(const Table& table);

        explicit Row(const Table& table, const vector<Block*>& data);

        ~Row();

        void InsertColumnData(Block* block, const size_t& columnIndex);

        const vector<Block*>& GetData() const;

        void PrintRow(const Database* db) const;
};



#endif //ROW_H
