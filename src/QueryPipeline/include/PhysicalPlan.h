#pragma once
#include <vector>
#include <string>

#include "Statements.h"
#include "../../Systemic/include/Errors.h"
#include "../../Systemic/include/QueryResult.h"
#include "../../Systemic/include/Headers.h"
#include "../../DatabaseEngine/include/DataStorage/Row.h"
#include "../../DatabaseEngine/include/ExecutionProperties.h"
#include "../../Systemic/include/DataStructures/PriorityQueue.h"
#include "../../DatabaseEngine/include/Algorithms/Sort/SortingFunctions.h"

struct MergeElement;

namespace DatabaseEngine {
  struct ExecutionProperties;
    class SystemCatalog;
  }

  namespace Network {
    struct Session;
  }

  namespace QueryPipeline {
    class LogicalPlan;
  }

  namespace DatabaseEngine::StorageTypes {
    class Row;
  }

namespace QueryPipeline::PhysicalPlan{
  struct ExecutionResult {
      std::vector<std::string> displayColumnNames;

      std::vector<const DatabaseEngine::StorageTypes::Column*> columns;

      std::vector<Pages::RowReference> rows;

      std::vector<QueryResult> results;

      std::string message;
      Errors::RuntimeError code;

      bool canFetchMore;

      ExecutionResult();
      ExecutionResult(const Errors::RuntimeError& code, const std::string& message);
      ~ExecutionResult();

      [[nodiscard]] bool IsOk()const;
  };

  class ExecutionNode {
    protected:
      DataTypes::Guid sessionId;

      DatabaseEngine::SystemCatalog* catalog;
      Network::Server *server;

      const Network::Session* session;

      Int temporaryTableId;

    public:
      ExecutionNode();
      explicit ExecutionNode(const DataTypes::Guid& currentSessionId);
      virtual ~ExecutionNode() = default;
      void InsertToTemporaryDatabase(const std::vector<DatabaseEngine::StorageTypes::Row>& rows);
      void InsertPostProjectionResultsToTemporaryDatabase(
        const DatabaseEngine::ExecutionProperties& properties,
        ExecutionResult*& result,
        DataTypes::RowIdentifier& firstRowId
      );
      [[nodiscard]] ExecutionResult* StreamFromTemporaryDatabase(
        const DatabaseEngine::ExecutionProperties& properties,
        DatabaseEngine::ScanState& state
      ) const;
      virtual ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) = 0;
      virtual void UpdateScanState(const DataTypes::RowIdentifier& rowId);

      [[nodiscard]] bool UsesExternalStorage() const;
  };

  /**
   * @name Catalog Altering Classes
   * Classes that alter system catalog such as creating users, databases, schemas, etc.
   * @{
   */

  class PhysicalCreateUser final : public ExecutionNode {
    std::string username;
    std::string password;
    std::string roleName;
    public:
      explicit PhysicalCreateUser(std::string& username, std::string& password, std::string& role);
      ~PhysicalCreateUser()override = default;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties)override;
  };

  class PhysicalGrantRole final : public ExecutionNode {
      std::string username;
      std::string roleName;
    public:
      explicit PhysicalGrantRole(const DataTypes::Guid& sessionId, std::string& username, std::string& roleName);
      ~PhysicalGrantRole()override = default;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties)override;
  };

  class PhysicalCreateDatabase final : public ExecutionNode{
      std::string dbName;
    public:
      explicit PhysicalCreateDatabase(const DataTypes::Guid& sessionId, std::string& name);
      ~PhysicalCreateDatabase() override = default;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalUseDatabase final : public ExecutionNode{
    DataTypes::Guid sessionId;
    Int databaseId;

    public:
      explicit PhysicalUseDatabase(const DataTypes::Guid& sessionId, Int databaseId);
      ~PhysicalUseDatabase() override = default;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalSchemaCreate final : public ExecutionNode{
    std::string schemaName;
    Int databaseId;

    public:
      explicit PhysicalSchemaCreate(const DataTypes::Guid& sessionId, Int databaseId, std::string& schemaName);
      ~PhysicalSchemaCreate() override = default;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalTableCreate final : public ExecutionNode{
    Statements::DataSource*  table;
    std::string constraintName;
    std::vector<Statements::NewColumn*> columns;
    Headers::Index primaryKey;

  public:
    PhysicalTableCreate(
      const DataTypes::Guid& sessionId,
      Statements::DataSource*  table,
      std::vector<Statements::NewColumn*>& columns,
      Headers::Index& primaryKey,
      std::string& constraintName
    );
    ~PhysicalTableCreate()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalIndexCreate final : public ExecutionNode {
    Statements::DataSource* table;
    std::string constraintName;
    std::vector<column_index_t> columns;

  public:
    PhysicalIndexCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource*  table,
        std::string& constraintName,
        std::vector<column_index_t>& columns
    );
    ExecutionResult * Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  /** @} End of Catalog Altering Classes */

  /**
  * @name Table Alter Classes
  * Classes that alter tables such as add column, drop column, rename column, etc.
  * @{
  */

  class PhysicalAddColumn final : public ExecutionNode {
    Statements::DataSource* table;
    Statements::NewColumn* column;

  public:
    PhysicalAddColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::NewColumn* column);
    ~PhysicalAddColumn()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalDropColumn final : public ExecutionNode {
    Statements::DataSource* table;
    Statements::DropColumn* column;

  public:
    PhysicalDropColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::DropColumn* column);
    ~PhysicalDropColumn()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalRenameColumn final : public ExecutionNode {
    Statements::DataSource* table;
    Statements::RenameColumn* column;

  public:
    PhysicalRenameColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::RenameColumn* column);
    ~PhysicalRenameColumn()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalAlterColumn final : public ExecutionNode {
    Statements::DataSource* table;
    Statements::AlterColumn* column;

  public:
    PhysicalAlterColumn(const DataTypes::Guid& sessionId, Statements::DataSource* table, Statements::AlterColumn* column);
    ~PhysicalAlterColumn()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  /** @} End of Table Alter Classes */

  /**
 * @name Table Scan Classes
 * Classes that scan tables such as table scan, index scan, index seek, etc.
 * @{
 */

  class PhysicalTableScan final : public ExecutionNode{
    Statements::DataSource* table;
    Expressions::Expression* expression;
    DatabaseEngine::ScanState state;

    public:
      explicit PhysicalTableScan(Statements::DataSource* table, Expressions::Expression* expression);
      ~PhysicalTableScan()override;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
      void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
  };

  class PhysicalIndexScan final : public ExecutionNode{
    Statements::DataSource* table;
    Expressions::Expression* expression;
    DatabaseEngine::IndexState state;
    bool isClustered;

  public:
    explicit PhysicalIndexScan(Statements::DataSource* table, bool isClustered = false);
    explicit PhysicalIndexScan(Statements::DataSource* table, Expressions::Expression* expression, bool isClustered = false);
    ~PhysicalIndexScan()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
    void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
  };

  class PhysicalIndexSeek final : public ExecutionNode{
    Statements::DataSource* table;
    Expressions::Expression* expression;
    DataTypes::Indexing::Key key;

  public:
    explicit PhysicalIndexSeek(
      Statements::DataSource* table,
      DataTypes::Indexing::Key& key,
      Expressions::Expression* expression
    );
    ~PhysicalIndexSeek()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalIndexSeekRange final : public ExecutionNode{
    Statements::DataSource* table;
    Expressions::Expression* expression;
    DataTypes::Indexing::Key minKey;
    DataTypes::Indexing::Key maxKey;

    public:
      explicit PhysicalIndexSeekRange(
        Statements::DataSource* table,
        DataTypes::Indexing::Key& minKey,
        DataTypes::Indexing::Key& maxKey,
        Expressions::Expression* expression
      );
      ~PhysicalIndexSeekRange()override;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  /** @} End of Table Scan Classes */

  /**
  * @name Select Processing Classes
  * Classes that process data retrieved from scans such as projection, filtering, top, distinct, etc.
  * @{
  */

  class PhysicalProject final : public ExecutionNode{
    std::vector<Expressions::Expression*> resultExpressions;
    std::vector<Headers::ColumnHeader> columnHeaders;
    ExecutionNode* child;


    [[nodiscard]] inline ExecutionResult* ExecuteStatement(const DatabaseEngine::ExecutionProperties& properties);
    [[nodiscard]] inline ExecutionResult* ExecuteConstantStatement(const DatabaseEngine::ExecutionProperties& properties)const;

    public:
      PhysicalProject(
        ExecutionNode* child,
        std::vector<Expressions::Expression*>& resultExpressions,
        std::vector<Headers::ColumnHeader>& columnHeaders);
      ~PhysicalProject() override;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
      void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
  };

  class PhysicalFilter final : public ExecutionNode{
    Expressions::Expression* filter;
    ExecutionNode* child;

    public:
      PhysicalFilter(ExecutionNode* child, Expressions::Expression* filter);
      ~PhysicalFilter() override;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
      void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
  };

  class PhysicalTop final : public ExecutionNode {
    int64_t top;
    ExecutionNode* child;

    public:
      PhysicalTop(ExecutionNode* child, BigInt top);
      ~PhysicalTop() override;

    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
    void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
  };

  class PhysicalDistinct final : public ExecutionNode {
    ExecutionNode* child;

    public:
      explicit PhysicalDistinct(ExecutionNode* child);
      ~PhysicalDistinct()override;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
      void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
  };

  class PhysicalOrderBy final : public ExecutionNode{
    ExecutionNode* child;
    std::vector<Statements::OrderColumn*> expressions;

    PriorityQueue<MergeElement, MergeComparator> priorityQueue;

    [[nodiscard]] bool CanBeSortedInMemory(bool canFetchMore)const;

  public:
    PhysicalOrderBy(ExecutionNode* child, std::vector<Statements::OrderColumn*>& expressions);
    ~PhysicalOrderBy()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
    void UpdateScanState(const DataTypes::RowIdentifier& rowId) override;
  };

  /** @} End of Select Processing Classes */

  /**
  * @name Insert and Update Classes
  * Classes that modify data such as insert, update, delete, etc.
  * @{
  */

  class PhysicalInsert final : public ExecutionNode{
    Statements::DataSource* table;
    std::vector<Statements::Inserts> fields;

    ExecutionNode* child;
    std::vector<column_index_t> columnsIndices;

    static bool SortInsertsAscending(const Value& lhs, const Value& rhs);

    std::vector<Value> ConvertExpressionsToValues(
      const DatabaseEngine::ExecutionProperties& properties,
      Int index
    )const;
    ExecutionResult* InsertFromChild(DatabaseEngine::StorageTypes::Table* tablePtr, const DatabaseEngine::ExecutionProperties& properties)const;
    ExecutionResult* InsertFromFields(DatabaseEngine::StorageTypes::Table* tablePtr, const DatabaseEngine::ExecutionProperties& properties);
  public:
    PhysicalInsert(
      Statements::DataSource* table,
      std::vector<Statements::Inserts>& fields,
      ExecutionNode* child,
      std::vector<column_index_t>& columnsIndices
    );
    ~PhysicalInsert()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalHeapUpdate final : public ExecutionNode{
    Statements::DataSource* table;
    std::vector<Statements::UpdateColumn*>  updates;
    Expressions::Expression* expression;

  public:
    PhysicalHeapUpdate(Statements::DataSource* table, Expressions::Expression* expression, std::vector<Statements::UpdateColumn*> & updates);
    ~PhysicalHeapUpdate()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalIndexScanUpdate final : public ExecutionNode{
    Statements::DataSource* table;
    std::vector<Statements::UpdateColumn*>  updates;
    Expressions::Expression* expression;

  public:
    PhysicalIndexScanUpdate(Statements::DataSource* table, Expressions::Expression* expression, std::vector<Statements::UpdateColumn*> & updates);
    ~PhysicalIndexScanUpdate()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalIndexSeekUpdate final : public ExecutionNode{
    Statements::DataSource* table;
    std::vector<Statements::UpdateColumn*>  updates;
    Expressions::Expression* expression;

  public:
    PhysicalIndexSeekUpdate(Statements::DataSource* table, Expressions::Expression* expression, std::vector<Statements::UpdateColumn*> & updates);
    ~PhysicalIndexSeekUpdate()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalHeapDelete final : public ExecutionNode{
    Statements::DataSource* table;
    Expressions::Expression* expression;

  public:
    PhysicalHeapDelete(Statements::DataSource* table, Expressions::Expression* expression);
    ~PhysicalHeapDelete()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalIndexScanDelete final : public ExecutionNode{
    Statements::DataSource* table;
    Expressions::Expression* expression;
    DatabaseEngine::IndexState state;

  public:
    PhysicalIndexScanDelete(Statements::DataSource* table, Expressions::Expression* expression);
    ~PhysicalIndexScanDelete()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalIndexSeekDelete final : public ExecutionNode{
    Statements::DataSource* table;
    Expressions::Expression* expression;
    DatabaseEngine::IndexState state;

  public:
    PhysicalIndexSeekDelete(Statements::DataSource* table, Expressions::Expression* expression);
    ~PhysicalIndexSeekDelete()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  /** @} End of Insert and Update Classes */

  /**
  * @name Join Classes
  * Classes that perform joins such as nested loop join, merge join, hash join, etc.
  * @{
  */

    class PhysicalNestedLoopInnerJoin final : public ExecutionNode {
    ExecutionNode* left;
    ExecutionNode* right;
    Expressions::Expression* expression;

    ExecutionResult* ExecuteBatchJoin(
      const DatabaseEngine::ExecutionProperties& properties,
      ExecutionResult* leftResult
    ) const;

    public:
      PhysicalNestedLoopInnerJoin(
        ExecutionNode* left,
        ExecutionNode* right,
        Expressions::Expression* joinCondition
      );
      ~PhysicalNestedLoopInnerJoin()override;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalMergeInnerJoin final : public ExecutionNode {
    ExecutionNode* left;
    ExecutionNode* right;
    Expressions::Expression* expression;

    std::vector<column_index_t> leftKeyColumns;
    std::vector<column_index_t> rightKeyColumns;

    ExecutionResult* ExecuteBatchJoin(
      const DatabaseEngine::ExecutionProperties& properties,
      ExecutionResult* leftResult
    ) const;

  public:
    PhysicalMergeInnerJoin(
      ExecutionNode* left,
      ExecutionNode* right,
      Expressions::Expression* expression,
      std::vector<column_index_t>& leftKeyColumns,
      std::vector<column_index_t>& rightKeyColumns
    );
    ~PhysicalMergeInnerJoin()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalMergeLeftJoin final : public ExecutionNode {
    ExecutionNode* left;
    ExecutionNode* right;
    Expressions::Expression* expression;

    std::vector<column_index_t> leftKeyColumns;
    std::vector<column_index_t> rightKeyColumns;

    ExecutionResult* ExecuteBatchJoin(
      const DatabaseEngine::ExecutionProperties& properties,
      ExecutionResult* leftResult
    ) const;

    public:
      PhysicalMergeLeftJoin(
        ExecutionNode* left,
        ExecutionNode* right,
        Expressions::Expression* expression,
        std::vector<column_index_t>& leftKeyColumns,
        std::vector<column_index_t>& rightKeyColumns
      );

      ~PhysicalMergeLeftJoin()override;
      ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalMergeFullJoin final : public ExecutionNode {
    ExecutionNode* left;
    ExecutionNode* right;
    Expressions::Expression* expression;

    std::vector<column_index_t> leftKeyColumns;
    std::vector<column_index_t> rightKeyColumns;

    ExecutionResult* ExecuteBatchJoin(
      const DatabaseEngine::ExecutionProperties& properties,
      ExecutionResult* leftResult
    ) const;

  public:
    PhysicalMergeFullJoin(
      ExecutionNode* left,
      ExecutionNode* right,
      Expressions::Expression* expression,
      std::vector<column_index_t>& leftKeyColumns,
      std::vector<column_index_t>& rightKeyColumns
    );

    ~PhysicalMergeFullJoin()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalNestedLoopLeftJoin final : public ExecutionNode {
    ExecutionNode* left;
    ExecutionNode* right;
    Expressions::Expression* expression;

  public:
    PhysicalNestedLoopLeftJoin(
      ExecutionNode* left,
      ExecutionNode* right,
      Expressions::Expression* expression
    );
    ~PhysicalNestedLoopLeftJoin()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  class PhysicalNestedLoopFullJoin final : public ExecutionNode {
    ExecutionNode* left;
    ExecutionNode* right;
    Expressions::Expression* joinCondition;

  public:
    PhysicalNestedLoopFullJoin(
      ExecutionNode* left,
      ExecutionNode* right,
      Expressions::Expression* joinCondition
    );
    ~PhysicalNestedLoopFullJoin()override;
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties) override;
  };

  /** @} End of Join Classes */

  /**
  * @name Variable Classes
  * Classes that declare and update session variables
  * @{
  */

  class PhysicalDeclareVariable final : public ExecutionNode {
    Variable variable;
    Expressions::Expression* expression;

  public:
    explicit PhysicalDeclareVariable(const DataTypes::Guid& currentSessionId, Variable& variable, Expressions::Expression* expression);
    ExecutionResult* Execute(const DatabaseEngine::ExecutionProperties& properties)override;
  };

  /** @} End of Variable Classes */
}