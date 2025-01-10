#pragma once
#include "../Page.h"
#include <cstdint>
#include <fstream>

using namespace std;

namespace Indexing {
	class BPlusTree;
	struct Node;
	struct Key;
	struct QueryData;
	struct NodeHeader;
} // namespace Indexing

namespace DatabaseEngine::StorageTypes {
	class Table;
}

namespace Storage {
	class PageManager;
}

namespace Pages {

	typedef struct IndexPageAdditionalHeader {
		TreeType treeType;
		page_id_t treeId;

		IndexPageAdditionalHeader();
		~IndexPageAdditionalHeader();
	}IndexPageAdditionalHeader;

	class IndexPage final : public Page {
		IndexPageAdditionalHeader additionalHeader;
		vector<Indexing::Node*> nodes; 
		vector<column_index_t> indexedColumns;

		protected:
			void WriteAdditionalHeaderToFile(fstream* filePtr) const;
			void ReadAdditionalHeaderFromFile(const vector<char>& data, page_offset_t &offSet);

		public:
			IndexPage(const page_id_t &pageId, const bool &isPageCreation);
			explicit IndexPage(const PageHeader &pageHeader);
			~IndexPage() override;

			void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
			void WritePageToFile(fstream *filePtr) override;

			void SetTreeType(const TreeType& treeType);
			void SetTreeId(const page_id_t& treeId);

			[[nodiscard]] const page_id_t& GetTreeId() const; 

			void InsertNode(Indexing::Node*& node, page_offset_t* indexPosition);

			void DeleteNode(const page_offset_t& indexPosition);

			[[nodiscard]] Indexing::Node*& GetNodeByIndex(const page_offset_t& indexPosition);

			[[nodiscard]] Indexing::Node* GetRoot();

			void UpdateNodeParentHeader(const page_offset_t& indexPosition, const Indexing::NodeHeader& nodeHeader);

			void UpdateNodeChildHeader(const page_offset_t& indexPosition, const page_offset_t& childIndexPosition, const Indexing::NodeHeader& nodeHeader);

			void UpdateNodeNextLeafHeader(const page_offset_t& indexPosition, const Indexing::NodeHeader& nodeHeader);

			void UpdateNodePreviousLeafHeader(const page_offset_t& indexPosition, const Indexing::NodeHeader& nodeHeader);
		};
} // namespace Pages
