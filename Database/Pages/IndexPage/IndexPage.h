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
} // namespace Indexing

namespace DatabaseEngine::StorageTypes {
class Table;
}

namespace Storage {
class PageManager;
}

namespace Pages {
class IndexPage final : public Page {
	vector<Indexing::Node*> nodes; 
	vector<column_index_t> indexedColumns;

public:
	IndexPage(const page_id_t &pageId, const bool &isPageCreation);
	explicit IndexPage(const PageHeader &pageHeader);
	~IndexPage() override;

	void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
	void WritePageToFile(fstream *filePtr) override;

	//void WriteTreeDataToPage(Indexing::Node *node, page_offset_t &offSet);

	void InsertNode(Indexing::Node*& node, page_offset_t* indexPosition);

	void DeleteNode(const page_offset_t& indexPosition);

	[[nodiscard]] Indexing::Node*& GetNodeByIndex(const page_offset_t& indexPosition);

	[[nodiscard]] Indexing::Node* GetRoot();
	};
} // namespace Pages
