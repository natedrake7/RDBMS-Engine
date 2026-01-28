#pragma once
#include "DataTypes/Value.h"


#include <vector>

class QueryResult {
  std::vector<Value> data;

public:
    QueryResult();
    QueryResult(const QueryResult& other);
    QueryResult(QueryResult&& other) noexcept;

    void AddColumn(Value& field);
    void AddColumn(const Value& field);
    void AddColumn(const Value& field, column_index_t columnIndex);
    void Print()const;
    [[nodiscard]] const std::vector<Value>& Data()const;
    [[nodiscard]] std::vector<Value>& Data();
    [[nodiscard]] Value GetColumnAt(Int columnPos)const;
    [[nodiscard]] Int GetSize()const;
    [[nodiscard]] Int GetByteSize()const;
    [[nodiscard]] Int GetPageByteSize()const;
    void SetColumnIndex(Int columnPos, column_index_t columnIndex);
    [[nodiscard]] BigInt ComputeHash()const;

    void Serialize(std::vector<char>& buffer)const;
    void Deserialize(const std::vector<char>& buffer, UnsignedInt& offset, Int dataSize);

    void Update(std::vector<Value>& updates);
    void Update(const std::vector<Value>& updates);
    void Update(Value& update);

    QueryResult& operator=(const QueryResult& other);
    QueryResult& operator=(QueryResult&& other) noexcept;

    friend bool operator==(const QueryResult& lhs, const QueryResult& rhs);
    friend std::ostream& operator<<(std::ostream& os, const QueryResult& result);
};
