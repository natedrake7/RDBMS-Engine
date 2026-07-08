#pragma once
#include "../../DatabaseConstants.h"
#include "../../Indexing/Key.h"
#include "../../Systemic/include/RowIdentifier.h"
#include "../../DataStorage/Row.h"
#include "../../../../Systemic/include/DataTypes/PackedWord.h"

namespace CoreEngine::StorageTypes{
    class SerializedRow;
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

		page_id_t previousNode;
		page_id_t nextNode;

		UnsignedTinyInt _flags;
        byte_t _reserved[Constants::GAM_HEADER_RESERVED_SPACE];

        [[nodiscard]] Constants::TreeType GetTreeType() const{
            return PackedByte::ExtractBits<Constants::TreeType>(_flags, TREE_TYPE_BIT_POS, TREE_TYPE_BIT_MASK);
        }

        [[nodiscard]] bool IsLeaf() const{
            return PackedByte::GetBit<IS_LEAF_BIT_POS>(this->_flags);
        }

        [[nodiscard]] bool IsRoot() const{
            return PackedByte::GetBit<IS_ROOT_BIT_POS>(this->_flags);
        }

        [[nodiscard]] bool IsEmpty() const{
            return PackedByte::GetBit<IS_EMPTY_BIT_POS>(this->_flags);
        }

        void SetTreeType(const Constants::TreeType type){
            PackedByte::SetBits<0, TREE_TYPE_BIT_MASK>(&this->_flags, type);
        }

        void SetIsLeaf(const bool value){
            PackedByte::SetBit<IS_LEAF_BIT_POS>(&this->_flags, value);
        }

        void SetIsRoot(const bool value){
            PackedByte::SetBit<IS_ROOT_BIT_POS>(&this->_flags, value);
        }

        void SetIsEmpty(const bool value){
            PackedByte::SetBit<IS_EMPTY_BIT_POS>(&this->_flags, value);
        }

		IndexPageAdditionalHeader()
		    :   _flags(0),
                previousNode(INVALID_PAGE_ID),
                nextNode(INVALID_PAGE_ID){
            this->SetTreeType(Constants::TreeType::NonClustered);
            this->SetIsLeaf(false);
            this->SetIsRoot(false);
            this->SetIsEmpty(true);
        }
	};

    struct IndexInsertTuple{
        DataTypes::Indexing::Key key;
        CoreEngine::StorageTypes::SerializedRow* payload;

        IndexInsertTuple();
        IndexInsertTuple(DataTypes::Indexing::Key& key, CoreEngine::StorageTypes::SerializedRow* payload);
    };

	struct LeafNodeTuple{
		DataTypes::Indexing::Key key;
		CoreEngine::StorageTypes::RID row;

		LeafNodeTuple& operator=(LeafNodeTuple&& other) noexcept;
		LeafNodeTuple(LeafNodeTuple&& other) noexcept;

		LeafNodeTuple& operator=(const LeafNodeTuple& other) = delete;
		LeafNodeTuple(const LeafNodeTuple& other) = delete;

		LeafNodeTuple(CoreEngine::StorageTypes::RID row, DataTypes::Indexing::Key& key);
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

		InternalNodeTuple(DataTypes::Indexing::Key& key, page_id_t pageId);

	    void SetKey(DataTypes::Indexing::Key& otherKey);
	    void SetPageId(page_id_t otherPageId);
	};
}
