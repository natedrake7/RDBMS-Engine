#pragma once
#include "../../Database/include/Database.h"
#include "PhysicalPlan.h"

namespace QueryPipeline {
  class LogicalPlan {
    public:
      DataTypes::Guid sessionId;
      int32_t databaseId;

      LogicalPlan(const DataTypes::Guid& sessionId, const int32_t& databaseId);
      explicit LogicalPlan(const DataTypes::Guid& sessionId);
      LogicalPlan();
      virtual ~LogicalPlan();
      virtual PhysicalPlan::ExecutionNode* ToPhysical() = 0;
  };

  class LogicalDeclareVariable final : public LogicalPlan {
    public:
      Variable variable;
      Expressions::Expression* expression;

      LogicalDeclareVariable(const DataTypes::Guid& sessionId, Variable& variable, Expressions::Expression* expression);
      PhysicalPlan::ExecutionNode * ToPhysical() override;
  };

  class LogicalCreateUser final : public LogicalPlan {
    public:
      std::string username;
      std::string password;
      std::string role;

    explicit LogicalCreateUser(
      const DataTypes::Guid& sessionId,
      std::string&  username,
      std::string & password,
      std::string & role
    );
    ~LogicalCreateUser()override;
    PhysicalPlan::ExecutionNode * ToPhysical() override;
  };

  class LogicalGrantRole final: public LogicalPlan {
    public:
      std::string username;
      std::string role;

    explicit LogicalGrantRole(const DataTypes::Guid& sessionId, std::string & username, std::string & role);
    ~LogicalGrantRole()override = default;
    PhysicalPlan::ExecutionNode * ToPhysical() override;
  };

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      std::string dbName;
      explicit LogicalCreateDatabase(const DataTypes::Guid& sessionId, std::string& dbName);
      PhysicalPlan::PhysicalCreateDatabase* ToPhysical()override;
  };

  class LogicalUseDatabase final : public LogicalPlan {
    public:
      int32_t databaseId;
      DataTypes::Guid sessionId;

      explicit LogicalUseDatabase(const DataTypes::Guid& sessionId, const int32_t& databaseId);
      PhysicalPlan::PhysicalUseDatabase* ToPhysical()override;
  };

  class LogicalProject final: public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<Expressions::Expression*> resultExpressions;
      std::vector<Headers::ColumnHeader> columnsHeaders;

      LogicalProject(
        LogicalPlan* child,
        std::vector<Expressions::Expression*>& resultExpressions,
        std::vector<Headers::ColumnHeader>& columnsHeaders);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      Expressions::Expression* expression;
      explicit LogicalTableScan(
        Statements::DataSource* table,
        Expressions::Expression* expression
      );
      PhysicalPlan::ExecutionNode* ToPhysical() override;
  };

  class LogicalJoin final : public LogicalPlan {
    public:
    LogicalPlan* left;
    LogicalPlan* right;
    Expressions::Expression* condition;
    JoinType type;
    LogicalJoin(
      LogicalPlan* left,
      LogicalPlan* right,
      Expressions::Expression* condition,
      const JoinType& type
    );

    ~LogicalJoin() override;

    PhysicalPlan::ExecutionNode* ToPhysical()override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Expressions::Expression* filter;

      explicit LogicalFilter( LogicalPlan* child, Expressions::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical()override;
  };

  class LogicalOrder final : public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<Statements::OrderColumn*> expressions;

      explicit LogicalOrder(
        LogicalPlan* child,
        std::vector<Statements::OrderColumn*>& expressions
      );
      PhysicalPlan::ExecutionNode* ToPhysical()override;
  };

  class LogicalTop final : public LogicalPlan {
    public:
      LogicalPlan* child;
      int64_t top;

      explicit LogicalTop(LogicalPlan* child, const int64_t& top);
      ~LogicalTop() override;
      PhysicalPlan::PhysicalTop* ToPhysical()override;
  };

  class LogicalDistinct final : public LogicalPlan {
    public:
      LogicalPlan* child;

      explicit LogicalDistinct(LogicalPlan* child);
      ~LogicalDistinct() override;
      PhysicalPlan::PhysicalDistinct* ToPhysical()override;
  };

  class LogicalInsert final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      std::vector<Statements::Inserts> fields;

      LogicalPlan* child;
      std::vector<column_index_t> columnsIndices;

      explicit LogicalInsert(
        Statements::DataSource* table,
        std::vector<Statements::Inserts>& fields,
        LogicalPlan* child,
        std::vector<column_index_t>& columnIndices
      );
      ~LogicalInsert()override;
      PhysicalPlan::PhysicalInsert* ToPhysical()override;
  };

  class LogicalSchemaCreate final : public LogicalPlan {
    public:
      std::string schemaName;
      int32_t databaseId;
      explicit LogicalSchemaCreate(const DataTypes::Guid& sessionId, const int32_t& databaseId, std::string& schemaName);
      PhysicalPlan::PhysicalSchemaCreate* ToPhysical()override;
  };

  class LogicalDelete final : public LogicalPlan {
  public:
    Statements::DataSource* table;
    Expressions::Expression* expression;
    explicit LogicalDelete(Statements::DataSource* table, Expressions::Expression* expression);
    PhysicalPlan::ExecutionNode* ToPhysical()override;
  };

  class LogicalUpdate final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      std::vector<Statements::UpdateColumn*> updates;
      Expressions::Expression* expression;

      explicit LogicalUpdate(Statements::DataSource* table, std::vector<Statements::UpdateColumn*>& updates, Expressions::Expression* expression);
      PhysicalPlan::ExecutionNode* ToPhysical()override;
  };

  class LogicalTableCreate final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      std::string constraintName;
      std::vector<Statements::NewColumn*> columns;
      vector<column_index_t> primaryKey;

      explicit LogicalTableCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        std::vector<Statements::NewColumn*>& columns,
        std::vector<column_index_t> primaryKey,
        std::string  constraintName);
      PhysicalPlan::PhysicalTableCreate* ToPhysical()override;
  };

  class LogicalIndexCreate final : public LogicalPlan {
    public:
    Statements::DataSource* table;
    std::string constraintName;
    std::vector<column_index_t> columns;
    explicit LogicalIndexCreate(
      const DataTypes::Guid& sessionId,
      Statements::DataSource* table,
      std::string& constraintName,
      std::vector<column_index_t>& columns
    );
    PhysicalPlan::ExecutionNode * ToPhysical() override;
  };

  class LogicalAlterTable final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      Constants::AlterTableType type;

      Statements::AlterColumn* alterColumn;
      Statements::DropColumn* dropColumn;
      Statements::RenameColumn* renameColumn;
      Statements::NewColumn* addColumn;

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const AlterTableType& type,
        Statements::AlterColumn* alterColumn,
        Statements::NewColumn* addColumn,
        Statements::DropColumn* dropColumn,
        Statements::RenameColumn* renameColumn);

      PhysicalPlan::ExecutionNode * ToPhysical() override;
  };
}

