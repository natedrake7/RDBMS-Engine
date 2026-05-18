#pragma once
#include <iosfwd>
#include <vector>

#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace CoreEngine::StorageTypes{
    class Table;
}

namespace CoreEngine::LoggingStructures {
  struct LogEntryBody {
    virtual ~LogEntryBody() = default;
    virtual void Serialize(std::vector<char>* buffer, page_offset_t& pos) = 0;
    virtual void Deserialize(const std::vector<char>* buffer, page_offset_t& pos, const StorageTypes::Table* table) = 0;
    [[nodiscard]] virtual int GetSize() const = 0;
    [[nodiscard]] virtual std::ostream& Print(std::ostream& os) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const LogEntryBody& logEntry);
  };

  // struct RowInsertBody final: LogEntryBody {
  //   StorageTypes::Row row;
  //   RowInsertBody();
  //   explicit RowInsertBody(const StorageTypes::Row& row);
  //   void Serialize(std::vector<char>* buffer, page_offset_t& pos)override;
  //   void Deserialize(const std::vector<char>* buffer, page_offset_t& pos, const StorageTypes::Table* table) override;
  //   [[nodiscard]] int GetSize() const override;
  //   [[nodiscard]] std::ostream& Print(std::ostream& os)const override;
  //   [[nodiscard]] const StorageTypes::Row& GetLastRowStatus() const override;
  // };
  //
  // struct BatchRowInsertBody final: LogEntryBody {
  //   const std::vector<StorageTypes::Row>* rows;
  //   BatchRowInsertBody();
  //
  //   explicit BatchRowInsertBody(const std::vector<StorageTypes::Row>& rows);
  //
  //   void Serialize(std::vector<char>* buffer, page_offset_t& pos)override;
  //   void Deserialize(const std::vector<char>* buffer, page_offset_t& pos, const StorageTypes::Table* table) override;
  //   [[nodiscard]] int GetSize() const override;
  //   [[nodiscard]] std::ostream& Print(std::ostream& os)const override;
  //   [[nodiscard]] const StorageTypes::Row& GetLastRowStatus() const override;
  // };
  //
  // struct RowUpdateBody final: LogEntryBody {
  //   StorageTypes::Row* oldRow;
  //   StorageTypes::Row* newRow;
  //   RowUpdateBody();
  //   RowUpdateBody(StorageTypes::Row* oldRow, StorageTypes::Row* newRow);
  //   void Serialize(std::vector<char>* buffer, page_offset_t& pos)override;
  //   void Deserialize(const std::vector<char>* buffer, page_offset_t& pos, const StorageTypes::Table* table) override;
  //   [[nodiscard]] int GetSize() const override;
  //   [[nodiscard]] std::ostream& Print(std::ostream& os)const override;
  //   [[nodiscard]] const StorageTypes::Row& GetLastRowStatus() const override;
  // };
  //
  // struct RowDeleteBody final: LogEntryBody {
  //   StorageTypes::Row* row;
  //   RowDeleteBody();
  //   explicit RowDeleteBody(StorageTypes::Row* row);
  //   void Serialize(std::vector<char>* buffer, page_offset_t& pos)override;
  //   void Deserialize(const std::vector<char>* buffer, page_offset_t& pos, const StorageTypes::Table* table) override;
  //   [[nodiscard]] int GetSize() const override;
  //   [[nodiscard]] std::ostream& Print(std::ostream& os)const override;
  //   [[nodiscard]] const StorageTypes::Row& GetLastRowStatus() const override;
  // };
  //
  // struct TableCreateBody final: LogEntryBody {
  //   std::string query;
  //
  //   explicit TableCreateBody();
  //   explicit TableCreateBody(const std::string& query);
  //   void Serialize(std::vector<char>* buffer, page_offset_t& pos)override;
  //   void Deserialize(const std::vector<char>* buffer, page_offset_t& pos, const StorageTypes::Table* table) override;
  //   [[nodiscard]] int GetSize() const override;
  //   [[nodiscard]] std::ostream& Print(std::ostream& os)const override;
  //   [[nodiscard]] const StorageTypes::Row& GetLastRowStatus() const override;
  // };
}
