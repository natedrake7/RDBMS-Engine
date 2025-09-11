#pragma once
#include "../../AdditionalLibraries/DataTypes/Value/Value.h"

#include <vector>
#include "../../Database/Constants.h"
#include "../../QueryPipeline/PhysicalPlan/PhysicalPlan.h"

#include <fstream>

#include "../Column/Column.h"
#include "../Row/Row.h"

namespace AdditionalDataTypes {
  struct ResultStatus;
}

using namespace std;
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
        Constants::DataType type;
        vector<Key> subKeys;

        Key();
        Key(const void *keyValue, const key_size_t &keySize, const Constants::DataType& keyType);
        explicit Key(const Value& field);

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

    class BPlusTree final
    {
        table_id_t tableId;
        int16_t tablePosition;
        page_id_t firstIndexPageId;

        int t;
        int keySize;

        Pages::IndexPage *root;

        TreeType type;
        int nonClusteredIndexId;

        DatabaseEngine::Database* database;
        DatabaseEngine::StorageTypes::Table* table;

        void SplitChild(Pages::IndexPage *parent, const int &index, Pages::IndexPage *child)const;
        Pages::IndexPage *GetNonFullNode(Pages::IndexPage *node, const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status);
        [[nodiscard]] Pages::IndexPage *SearchKey(const Key &key) const;
        [[nodiscard]] Pages::IndexPage *SearchKeyWithAncestors(const Key &key, std::vector<Pages::IndexPage*>& ancestors) const;
        [[nodiscard]] Pages::IndexPage *SearchLeftMostLeafNode() const;

        [[nodiscard]] Pages::IndexPage * GetNode(const page_id_t& pageId) const;
        [[nodiscard]] int CalculateTreeDegree(const DatabaseEngine::StorageTypes::Table* table, const TreeType& treeType, const int& nonClusteredIndexId)const;

        [[nodiscard]] Pages::IndexPage* AllocateNewPage(const page_id_t& parentPageId)const;

        void HandleUnderflow(Pages::IndexPage* node, const std::vector<Pages::IndexPage*>& ancestors, int& parentIndex);
        void HandleRootUnderflow();

        bool TryBorrowFromLeftSibling(Pages::IndexPage* node, Pages::IndexPage* parent, const int& index)const;
        bool TryBorrowFromRightSibling(Pages::IndexPage* node, Pages::IndexPage* parent, const int& index)const;

        void MergeNodes(
          Pages::IndexPage* leftNode,
          Pages::IndexPage* rightNode,
          Pages::IndexPage* parent,
          int parentKeyIndex,
          const std::vector<Pages::IndexPage*>& ancestors,
          int& parentIndex);

    public:
        explicit BPlusTree(DatabaseEngine::StorageTypes::Table *table, const page_id_t& indexPageId, const TreeType& treeType, const int& nonClusteredIndexId = -1);
        BPlusTree();
        ~BPlusTree();

        Pages::IndexPage *FindAppropriateNodeForInsert(const Key &key, int *indexPosition, AdditionalDataTypes::ResultStatus& status);

        void IndexSeek(const Key &minKey, const Key &maxKey, vector<QueryData> &result) const;

        void IndexSeek(const Key &minKey, const Key &maxKey, vector<DatabaseEngine::StorageTypes::Row>* result);

        void IndexScan(vector<QueryData> &result)const;

        void IndexScan(
            vector<DatabaseEngine::StorageTypes::Row>* result,
            QueryPipeline::PhysicalPlan::IndexState& state,
            const int& rowsToSelect);

        void IndexScan(
            vector<DatabaseEngine::StorageTypes::Row>* result,
            QueryPipeline::PhysicalPlan::IndexState& state,
            const int& rowsToSelect,
            const Expressions::Expression* expression);

        void IndexScan(
            vector<DatabaseEngine::StorageTypes::Row>* result,
            const Expressions::Expression* expression);

        void IndexScan(vector<DatabaseEngine::StorageTypes::Row>* result);

        void IndexScan(
            vector<Headers::RowIdentifier>* result,
            QueryPipeline::PhysicalPlan::IndexState& state,
            const int& rowsToSelect);

        void IndexScan(vector<Headers::RowIdentifier>* result, const Expressions::Expression* expression);

        void IndexScanUpdate(const Expressions::Expression* expression, const vector<Value> & updates);

        void IndexSeekUpdate(Expressions::Expression* expression, const Key* minKey, const Key* maxKey, const vector<Value> & updates);

        void SearchKey(const Key &key, QueryData &result) const;

        void Remove(const Key& key);

        Pages::IndexPage*& GetRoot();

        void SetRoot(Pages::IndexPage *&node);

        void SetBranchingFactor(const int &branchingFactor);

        [[nodiscard]] const int &GetBranchingFactor() const;

        void SetTreeType(const TreeType& treeType);

        [[nodiscard]] const page_id_t& GetFirstIndexPageId() const;

        void InsertRowsToOtherTree(const int& indexPos);

        void InsertColumnToRow(const Constants::column_index_t& index, const Value& defaultValue);

        void RemoveColumnFromRow(const Constants::column_index_t& index);
    };
}
