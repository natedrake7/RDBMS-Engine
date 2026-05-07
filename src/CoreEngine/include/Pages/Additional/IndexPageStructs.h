#pragma once
#include <array>

#include "../../Systemic/include/Key.h"
#include "../../Systemic/include/RowIdentifier.h"
#include "RowReference.h"

namespace CoreEngine::StorageTypes{
    class InsertPayload;
}

namespace Pages{
    struct IndexPageAdditionalHeader {
		static constexpr UnsignedTinyInt TREE_TYPE_BIT_POS = 0;
		static constexpr UnsignedTinyInt IS_LEAF_BIT_POS = 2;
		static constexpr UnsignedTinyInt IS_ROOT_BIT_POS = 3;
		static constexpr UnsignedTinyInt IS_EMPTY_BIT_POS = 4;
		static constexpr UnsignedTinyInt NUMBER_OF_SUB_KEYS_BIT_POS = 5;

		static constexpr UnsignedTinyInt TREE_TYPE_BIT_MASK = 0x03;        // 0000 0011
		static constexpr UnsignedTinyInt NUMBER_OF_SUB_KEYS_BIT_MASK = 0x07; //

        DataType keyTypes[Constants::MAX_NUMBER_OF_SUB_KEYS];
		PackedByte flags;
		page_id_t treeId;

		page_id_t previousNode;
		page_id_t nextNode;

        [[nodiscard]] Constants::TreeType GetTreeType() const;
		[[nodiscard]] bool IsLeaf() const;
		[[nodiscard]] bool IsRoot() const;
		[[nodiscard]] bool IsEmpty() const;
		[[nodiscard]] UnsignedTinyInt SubKeys() const;

		// Setters
		void SetTreeType(Constants::TreeType type);
		void SetIsLeaf(bool value);
		void SetIsRoot(bool value);
		void SetIsEmpty(bool value);
		void SetNumberOfSubKeys(UnsignedTinyInt count);

		IndexPageAdditionalHeader();
	};

    struct IndexInsertTuple{
        DataTypes::Indexing::Key key;
        CoreEngine::StorageTypes::InsertPayload* payload;

        IndexInsertTuple();
        IndexInsertTuple(DataTypes::Indexing::Key& key, CoreEngine::StorageTypes::InsertPayload* payload);
        ~IndexInsertTuple();
    };

	struct LeafNodeTuple{
		DataTypes::Indexing::Key key;
		RowReference row;

		LeafNodeTuple& operator=(LeafNodeTuple&& other) noexcept;
		LeafNodeTuple(LeafNodeTuple&& other) noexcept;

		LeafNodeTuple& operator=(const LeafNodeTuple& other) = delete;
		LeafNodeTuple(const LeafNodeTuple& other) = delete;

		LeafNodeTuple(RowReference& row, DataTypes::Indexing::Key& key);
	};

	struct RowIdTuple{
		DataTypes::Indexing::Key key;
		DataTypes::RowIdentifier rowId;
	};

	struct InternalNodeTuple{
		DataTypes::Indexing::Key key;
		page_id_t pageId;

	    InternalNodeTuple();

		InternalNodeTuple& operator=(InternalNodeTuple&& other) noexcept;
		InternalNodeTuple(InternalNodeTuple&& other) noexcept;

		InternalNodeTuple& operator=(const InternalNodeTuple& other);
		InternalNodeTuple(const InternalNodeTuple& other);

		InternalNodeTuple(DataTypes::Indexing::Key& key, page_id_t pageId);

	    void SetKey(DataTypes::Indexing::Key& otherKey);
	    void SetPageId(page_id_t otherPageId);
	};
}
