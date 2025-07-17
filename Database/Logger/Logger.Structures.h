#pragma once
#include <vector>
#include "../Row/Row.h"

namespace DatabaseEngine::LoggingStructures {
  struct LogEntryBody {
    virtual ~LogEntryBody() {}
    virtual void Serialize(std::vector<char>* buffer, uint32_t& pos) = 0;
    virtual void Deserialize(const std::vector<char>* buffer, uint32_t& pos) = 0;
    [[nodiscard]] virtual int GetSize() const = 0;
  };

  struct RowInsertBody final: public LogEntryBody {
    StorageTypes::Row* row;
    explicit RowInsertBody(StorageTypes::Row* row);
    void Serialize(std::vector<char>* buffer, uint32_t& pos)override;
    void Deserialize(const std::vector<char>* buffer, uint32_t& pos) override;
    [[nodiscard]] int GetSize() const override;
  };

  struct RowUpdateBody final: public LogEntryBody {
    StorageTypes::Row* oldRow;
    StorageTypes::Row* newRow;
    RowUpdateBody(StorageTypes::Row* oldRow, StorageTypes::Row* newRow);
    void Serialize(std::vector<char>* buffer, uint32_t& pos)override;
    void Deserialize(const std::vector<char>* buffer, uint32_t& pos) override;
    [[nodiscard]] int GetSize() const override;
  };

  struct RowDeleteBody final: public LogEntryBody {
    StorageTypes::Row* row;
    explicit RowDeleteBody(StorageTypes::Row* row);
    void Serialize(std::vector<char>* buffer, uint32_t& pos)override;
    void Deserialize(const std::vector<char>* buffer, uint32_t& pos) override;
    [[nodiscard]] int GetSize() const override;
  };

  struct TableCreateBody final: public LogEntryBody {
    std::string query;

    explicit TableCreateBody();
    explicit TableCreateBody(const std::string& query);
    void Serialize(std::vector<char>* buffer, uint32_t& pos)override;
    void Deserialize(const std::vector<char>* buffer, uint32_t& pos) override;
    [[nodiscard]] int GetSize() const override;
  };
}