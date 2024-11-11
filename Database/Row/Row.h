#ifndef ROW_H
#define ROW_H
#include "../Table/Table.h"
#include "../Block/Block.h"

class Block;
class Table;

using namespace std;

class Row {
    private:
        vector<Block*> data;
        const Table* table;

    public:
        explicit Row(const Table& table);

        explicit Row(const Table& table, const vector<Block*>& data);

        const vector<Block*>& GetData() const;

};



#endif //ROW_H
