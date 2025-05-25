#pragma once
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"


#include <vector>
#include "../../Database/Constants.h"
#include <fstream>

#include "../Column/Column.h"

namespace AdditionalDataTypes {
struct ResultStatus;}using namespace std;
using namespace Constants;

namespace DatabaseEngine 
{
    class Database;
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

        static page_size_t GetNonClusteredDataSize();
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
        Constants::ColumnType type;
        vector<Key> subKeys;

        Key();
        Key(const void *keyValue, const key_size_t &keySize, const Constants::ColumnType& keyType);
        Key(const Field& field);

        explicit Key(const vector<Key>& subKeys);
        ~Key();

        Key(const Key &otherKey);
        bool operator==(const Key& otherKey) const;
        bool operator>(const Key& otherKey) const;
        bool operator<(const Key& otherKey) const;
        bool operator<=(const Key& otherKey) const;
        bool operator>=(const Key& otherKey) const;

        [[nodiscard]] int GetKeySize() const;
        [[nodiscard]] int CompareCompositeKeys(const Key& otherKey) const;
        void InsertKey(const Key &otherKey);

        static int CompareSubKeys(const Key& firstKey, const Key& otherKey);

        //key comparison index used only on queries and not on key saveon db
        int indexKeyPosition = -1;
        int currentSearchKeyPosition = -1;

        friend std::ostream& operator<<(std::ostream& os, const Key& key);
    }Key;



    typedef struct NodeHeader{
        page_id_t pageId;
        page_offset_t indexPosition;

        NodeHeader();
        NodeHeader(const page_id_t& pageId, const page_offset_t& indexPosition);
        NodeHeader(const NodeHeader& otherHeader);
        ~NodeHeader();
        static page_size_t GetNodeHeaderSize();
    }NodeHeader;

    typedef struct Node
    {
        NodeHeader header;
        bool isLeaf;
        bool isRoot;
        bool isNodeClustered;
        
        vector<Key> keys;
        
        page_id_t dataPageId;
        vector<BPlusTreeNonClusteredData> nonClusteredData;
        
        NodeHeader parentHeader;
        vector<NodeHeader> childrenHeaders;
        NodeHeader nextNodeHeader;
        NodeHeader previousNodeHeader;

        page_size_t prevNodeSize;
        page_size_t currentNodeSize;

        explicit Node(const bool &isLeaf = false, const bool& isRoot = false, const bool& isNodeClustered = false);
        [[nodiscard]] page_size_t GetNodeSize() const;
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
        DatabaseEngine::StorageTypes::Table* table;

        void SplitChild(Node *parent, const int &index, Node *child);
        void PrintTree(const Node *node, const int &level);
        Node *GetNonFullNode(Node *node, const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status);
        void DeleteNode(const Node *node);
        [[nodiscard]] Node *SearchKey(const Key &key) const;
        [[nodiscard]] Node* SearchLeftMostLeafNode() const;
        void InsertNodeToPage(Node*& node, const page_id_t& parentPageId);

        [[nodiscard]] Node* GetNodeFromPage(const NodeHeader& header) const;
        static int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* table, const TreeType& treeType, const int& nonClusteredIndexId);

        void HandleUnderflow(Node* node);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Node* node, Node* parent, const int& index)const;
        bool TryBorrowFromRightSibling(Node* node, Node* parent, const int& index)const;

        void MergeNodes(Node* leftNode, Node* rightNode, Node* parent, int parentKeyIndex);

    public:
        explicit BPlusTree(DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId = -1);
        BPlusTree();
        ~BPlusTree();

        Node *FindAppropriateNodeForInsert(const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status);
        void PrintTree();

        void IndexSeek(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const;
        void IndexScan(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const;
        void IndexScan(vector<QueryData> &result) const;
        
        void SearchKey(const Key &key, QueryData &result) const;
        [[nodiscard]] page_size_t GetTreeSize() const;


        void Remove(const Key& key);

        Node*& GetRoot();

        void SetRoot(Node *&node);

        void SetBranchingFactor(const int &branchingFactor);

        [[nodiscard]] const int &GetBranchingFactor() const;

        void WriteTreeHeaderToFile(fstream *filePtr) const;

        void ReadTreeHeaderFromFile(const vector<char> &data, page_offset_t &offSet);

        void GetNodeSize(const Node *node, page_size_t &size) const;

        void SetTreeType(const TreeType& treeType);

        void UpdateRowData(const Key& key, const BPlusTreeNonClusteredData& data) const;

        [[nodiscard]] const page_id_t& GetFirstIndexPageId() const;
    };
}
