#pragma once
#include "IndexPage.h"
#include "PageView.h"


namespace Pages{
    struct IndexPageAdditionalHeader {
		static constexpr UnsignedTinyInt TREE_TYPE_BIT_POS = 0;
		static constexpr UnsignedTinyInt IS_LEAF_BIT_POS = 2;
		static constexpr UnsignedTinyInt IS_ROOT_BIT_POS = 3;
		static constexpr UnsignedTinyInt IS_EMPTY_BIT_POS = 4;
		static constexpr UnsignedTinyInt NUMBER_OF_SUB_KEYS_BIT_POS = 5;

		static constexpr UnsignedTinyInt TREE_TYPE_BIT_MASK = 0x03;        // 0000 0011
		static constexpr UnsignedTinyInt NUMBER_OF_SUB_KEYS_BIT_MASK = 0x07; //

		std::array<DataType, Constants::MAX_NUMBER_OF_SUB_KEYS> keyTypes;
		PackedByte flags;
		page_id_t treeId;

		page_id_t previousNode;
		page_id_t nextNode;

		Constants::TreeType GetTreeType() const;
		bool IsLeaf() const;
		bool IsRoot() const;
		bool IsEmpty() const;
		UnsignedTinyInt SubKeys() const;

		// Setters
		void SetTreeType(const Constants::TreeType& type);
		void SetIsLeaf(bool value);
		void SetIsRoot(bool value);
		void SetIsEmpty(bool value);
		void SetNumberOfSubKeys(UnsignedTinyInt count);

		IndexPageAdditionalHeader();
	};

    struct IndexInsertTuple{
        DataTypes::Indexing::Key key;
        DatabaseEngine::StorageTypes::InsertPayload* payload;

        IndexInsertTuple();
        IndexInsertTuple(DataTypes::Indexing::Key& key, DatabaseEngine::StorageTypes::InsertPayload* payload);
        ~IndexInsertTuple();
    };

	struct LeafNodeTuple{
		DataTypes::Indexing::Key key;
		RowReference row;

		LeafNodeTuple& operator=(LeafNodeTuple&& other) noexcept;
		LeafNodeTuple(LeafNodeTuple&& other) noexcept;

		LeafNodeTuple& operator=(const LeafNodeTuple& other);
		LeafNodeTuple(const LeafNodeTuple& other);

		LeafNodeTuple(RowReference& row, DataTypes::Indexing::Key& key);
	};

	struct RowIdTuple{
		DataTypes::Indexing::Key key;
		DataTypes::RowIdentifier rowId;
	};

	struct InternalNodeTuple{
		DataTypes::Indexing::Key key;
		page_id_t pageId;

		InternalNodeTuple& operator=(InternalNodeTuple&& other) noexcept;
		InternalNodeTuple(InternalNodeTuple&& other) noexcept;

		InternalNodeTuple& operator=(const InternalNodeTuple& other);
		InternalNodeTuple(const InternalNodeTuple& other);

		InternalNodeTuple(DataTypes::Indexing::Key& key, page_id_t pageId);
	};

    class IndexPageView final : public PageView {
        IndexPageAdditionalHeader* additionalHeaderPtr;

            void InsertFirstChild(page_id_t child) const;

            void InsertFirstKey(const DataTypes::Indexing::Key& key) const;
            void InsertKey(const DataTypes::Indexing::Key& key) const;

            void InsertFirstTuple(const IndexInsertTuple& tuple) const;

            DataTypes::Indexing::Key GetKey(page_offset_t& offSet) const;

        public:
            IndexPageView();
            explicit IndexPageView(Frame* framePtr);

            void SetTreeType(TreeType treeType) const;
            void SetTreeId(page_id_t treeId) const;
            void SetKeyTypes(const std::vector<DataType>& keyTypes) const;

            [[nodiscard]] bool IsEmpty() const;
            [[nodiscard]] bool IsLeaf() const;
            [[nodiscard]] bool IsRoot() const;
            [[nodiscard]] UnsignedTinyInt SubKeys() const;

            void SetIsLeaf(bool isLeaf) const;
            void SetIsRoot(bool isRoot) const;
            void SetSubKeys(UnsignedTinyInt numberOfKeys) const;

            void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key) const;
            void InsertChild(page_id_t child, const DataTypes::Indexing::Key* key, Int indexPosition) const;
            void InsertChild(page_id_t child) const;

            void SetLeftSibling(page_id_t previousPage) const;
            void SetRightSibling(page_id_t nextPage) const;

            [[nodiscard]] page_id_t LeftSibling()const;
            [[nodiscard]] page_id_t RightSibling()const;

            [[nodiscard]] bool HasLeftSibling() const;
            [[nodiscard]] bool HasRightSibling() const;

            void InsertKey(const DataTypes::Indexing::Key& key, Int indexPosition) const;

            void InsertTuple(const IndexInsertTuple& tuple) const;
            void InsertTuple(const IndexInsertTuple& tuple, Int indexPosition) const;

            DataTypes::Indexing::Key GetKey(Int indexPosition) const;
            LeafNodeTuple PeekLeafTuple(Int indexPosition) const;
            InternalNodeTuple PeekInternalNodeTuple(Int indexPosition) const;

            DatabaseEngine::StorageTypes::RowVersioningHeader PeekVersionHeader(Int indexPosition, Int& outKeySize) const;

            page_id_t GetChild(Int indexPosition) const;

            void AppendRowToBuffer(
                std::vector<RowReference>* buffer,
                const DatabaseEngine::StorageTypes::Table* table,
                const DatabaseEngine::Snapshot& snapshot,
                Int indexPosition
            );
    };
}
