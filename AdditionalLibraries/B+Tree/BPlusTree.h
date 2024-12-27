#pragma once
#include <vector>
#include "../../Database/Constants.h"
#include <fstream>

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes
{
    class Table;
}

namespace Indexing
{

    typedef struct BPlusTreeData
    {
        page_id_t pageId;
        extent_id_t extentId;

        BPlusTreeData();
        ~BPlusTreeData();
    }BPlusTreeData;

    typedef struct QueryData
    {
        BPlusTreeData treeData;
        int indexPosition;

        QueryData();
        QueryData(const BPlusTreeData &otherTreeData, const int &otherIndexPosition);
        ~QueryData();
    } QueryData;

    typedef struct Key
    {
        vector<object_t> value;
        key_size_t size;
        KeyType type;

        Key();
        Key(const void *keyValue, const key_size_t &keySize, const KeyType& keyType);
        ~Key();

        Key(const Key &otherKey);
        bool operator==(const Key& otherKey) const;
        bool operator>(const Key& otherKey) const;
        bool operator<(const Key& otherKey) const;
        bool operator<=(const Key& otherKey) const;
        bool operator>=(const Key& otherKey) const;
    } Key;

    struct Node
    {
        bool isLeaf;
        vector<Key> keys;
        BPlusTreeData data;
        vector<Node *> children;
        Node *next;
        Node *prev;

        explicit Node(const bool &isLeaf = false);
        vector<Key> &GetKeysData();
        BPlusTreeData &GetData();
        page_size_t GetNodeSize();
        ~Node();
    };

    class BPlusTree final
    {
        table_id_t tableId;
        int t;
        Node *root;
        bool isDirty;

        void SplitChild(Node *parent, const int &index, Node *child, vector<pair<Node *, Node *>> *splitLeaves) const;
        void PrintTree(const Node *node, const int &level);
        Node *GetNonFullNode(Node *node, const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves);
        void DeleteNode(Node *node);
        Node *SearchKey(const Key &key) const;

    public:
        explicit BPlusTree(const DatabaseEngine::StorageTypes::Table *table);
        BPlusTree();
        ~BPlusTree();

        Node *FindAppropriateNodeForInsert(const Key &key, int *indexPosition, vector<pair<Node *, Node *>> *splitLeaves);
        void PrintTree();

        void RangeQuery(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const;
        void SearchKey(const Key &key, QueryData &result) const;
        [[nodiscard]] page_size_t GetTreeSize() const;

        Node *GetRoot();

        void SetRoot(Node *&node);

        void SetBranchingFactor(const int &branchingFactor);

        const int &GetBranchingFactor() const;

        void WriteTreeHeaderToFile(fstream *filePtr);

        void ReadTreeHeaderFromFile(const vector<char> &data, page_offset_t &offSet);

        void GetNodeSize(const Node *node, page_size_t &size) const;

        const bool& IsTreeDirty() const;
    };
}
