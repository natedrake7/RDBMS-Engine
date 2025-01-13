﻿#pragma once
#include <vector>
#include "../../Database/Constants.h"
#include <fstream>

using namespace std;
using namespace Constants;

namespace DatabaseEngine 
{
    class Database;
}

namespace DatabaseEngine::StorageTypes
{
    class Table;
}

namespace Pages
{
    class IndexPage;
}

namespace Indexing
{
    typedef struct BPlusTreeNonClusteredData
    {
        page_id_t pageId;
        page_offset_t index;
        BPlusTreeNonClusteredData();
        BPlusTreeNonClusteredData(const page_id_t& pageId, const page_offset_t& index);
        ~BPlusTreeNonClusteredData();
    }BPlusTreeNonClusteredData;

    typedef struct QueryData
    {
        page_id_t pageId;
        page_offset_t indexPosition;

        QueryData();
        QueryData(const page_id_t &pageId, const page_offset_t &otherIndexPosition);
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
    }Key;

    typedef struct NodeHeader{
        page_id_t pageId;
        page_offset_t indexPosition;

        NodeHeader();
        ~NodeHeader();
        static page_size_t GetNodeHeaderSize();
    }NodeHeader;

    typedef struct Node
    {
        NodeHeader header;
        bool isLeaf;
        bool isRoot;
        
        vector<Key> keys;
        
        page_id_t dataPageId;
        vector<BPlusTreeNonClusteredData> nonClusteredData;
        
        NodeHeader parentHeader;
        vector<NodeHeader> childrenHeaders;
        NodeHeader nextNodeHeader;
        NodeHeader previousNodeHeader;

        page_size_t prevNodeSize;

        explicit Node(const bool &isLeaf = false, const bool& isRoot = false);
        page_size_t GetNodeSize();
        ~Node();
    }Node;

    class BPlusTree final
    {
        table_id_t tableId;
        page_id_t firstIndexPageId;
        int t;
        Node *root;
        TreeType type;
        int nonClusteredIndexId;
        DatabaseEngine::Database* database;

        void SplitChild(Node *parent, const int &index, Node *child, vector<tuple<Node*, Node *, Node *>> *splitLeaves);
        void PrintTree(const Node *node, const int &level);
        Node *GetNonFullNode(Node *node, const Key &key, int *indexPosition, vector<tuple<Node*, Node *, Node *>> *splitLeaves);
        void DeleteNode(Node *node);
        Node *SearchKey(const Key &key) const;
        void InsertNodeToPage(Node*& node);
        Node* GetNodeFromPage(const NodeHeader& header) const;

    public:
        explicit BPlusTree(const DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId = -1);
        BPlusTree();
        ~BPlusTree();

        Node *FindAppropriateNodeForInsert(const Key &key, int *indexPosition, vector<tuple<Node*, Node *, Node *>> *splitLeaves);
        void PrintTree();

        void RangeQuery(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const;
        void RangeQuery(const Key &minKey, const Key &maxKey, vector<BPlusTreeNonClusteredData> &result) const;
        void SearchKey(const Key &key, QueryData &result) const;
        [[nodiscard]] page_size_t GetTreeSize() const;

        Node*& GetRoot();

        void SetRoot(Node *&node);

        void SetBranchingFactor(const int &branchingFactor);

        const int &GetBranchingFactor() const;

        void WriteTreeHeaderToFile(fstream *filePtr) const;

        void ReadTreeHeaderFromFile(const vector<char> &data, page_offset_t &offSet);

        void GetNodeSize(const Node *node, page_size_t &size) const;

        void SetTreeType(const TreeType& treeType);

        void UpdateRowData(const Key& key, const BPlusTreeNonClusteredData& data);

        const page_id_t& GetFirstIndexPageId() const;
    };
}
