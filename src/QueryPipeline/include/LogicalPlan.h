#pragma once
#include "PhysicalPlan.h"

namespace QueryPipeline {
  struct JoinAlgorithmAnalysisResult;

  class LogicalPlan {
    public:
      DataTypes::Guid sessionId;
      Int databaseId;

      LogicalPlan(const DataTypes::Guid& sessionId, Int databaseId);
      explicit LogicalPlan(const DataTypes::Guid& sessionId);
      LogicalPlan();
      virtual ~LogicalPlan();
      virtual PhysicalPlan::ExecutionNode* ToPhysical(CompileResult& context) = 0;
  };

  class LogicalDeclareVariable final : public LogicalPlan {
    public:
      Variable variable;
      Expressions::Expression* expression;

      LogicalDeclareVariable(const DataTypes::Guid& sessionId, Variable& variable, Expressions::Expression* expression);
      PhysicalPlan::ExecutionNode * ToPhysical(CompileResult& context) override;
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
    PhysicalPlan::ExecutionNode * ToPhysical(CompileResult& context) override;
  };

  class LogicalGrantRole final: public LogicalPlan {
    public:
      std::string username;
      std::string role;

    explicit LogicalGrantRole(const DataTypes::Guid& sessionId, std::string & username, std::string & role);
    ~LogicalGrantRole()override = default;
    PhysicalPlan::ExecutionNode * ToPhysical(CompileResult& context) override;
  };

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      std::string dbName;
      explicit LogicalCreateDatabase(const DataTypes::Guid& sessionId, std::string& dbName);
      PhysicalPlan::PhysicalCreateDatabase* ToPhysical(CompileResult& context)override;
  };

  class LogicalUseDatabase final : public LogicalPlan {
    public:
      Int databaseId;
      DataTypes::Guid sessionId;

      explicit LogicalUseDatabase(const DataTypes::Guid& sessionId, Int databaseId);
      PhysicalPlan::PhysicalUseDatabase* ToPhysical(CompileResult& context)override;
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
      PhysicalPlan::PhysicalProject* ToPhysical(CompileResult& context)override;
  };

  class LogicalTableScan final : public LogicalPlan {
    [[nodiscard]] bool HasPredicate()const;

    public:
      Statements::DataSource* table;
      Expressions::Expression* expression;
      explicit LogicalTableScan(
        Statements::DataSource* table,
        Expressions::Expression* expression
      );
      PhysicalPlan::ExecutionNode* ToPhysical(CompileResult& context) override;
  };

  class LogicalJoin final : public LogicalPlan {
    PhysicalPlan::ExecutionNode* CreateInnerJoinPhysicalPlan(CompileResult& context, JoinAlgorithmAnalysisResult& analysis) const;
    PhysicalPlan::ExecutionNode* CreateLeftJoinPhysicalPlan(CompileResult& context, JoinAlgorithmAnalysisResult& analysis) const;
    PhysicalPlan::ExecutionNode* CreateRightJoinPhysicalPlan(CompileResult& context, JoinAlgorithmAnalysisResult& analysis) const;
    PhysicalPlan::ExecutionNode* CreateFullJoinPhysicalPlan(CompileResult& context, JoinAlgorithmAnalysisResult& analysis) const;

    public:
      Int leftTableId;
      Int rightTableId;

      LogicalPlan* left;
      LogicalPlan* right;
      Expressions::Expression* condition;
      JoinType type;
    LogicalJoin(
      LogicalPlan* left,
      LogicalPlan* right,
      Expressions::Expression* condition,
      JoinType type,
      Int leftTableId,
      Int rightTableId
    );

    ~LogicalJoin() override;

    PhysicalPlan::ExecutionNode* ToPhysical(CompileResult& context)override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Expressions::Expression* filter;

      explicit LogicalFilter( LogicalPlan* child, Expressions::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical(CompileResult& context)override;
  };

  class LogicalOrder final : public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<Statements::OrderColumn*> expressions;

      explicit LogicalOrder(
        LogicalPlan* child,
        std::vector<Statements::OrderColumn*>& expressions
      );
      PhysicalPlan::ExecutionNode* ToPhysical(CompileResult& context)override;
  };

  class LogicalTop final : public LogicalPlan {
    public:
      LogicalPlan* child;
      BigInt top;

      explicit LogicalTop(LogicalPlan* child, BigInt top);
      ~LogicalTop() override;
      PhysicalPlan::PhysicalTop* ToPhysical(CompileResult& context)override;
  };

  class LogicalDistinct final : public LogicalPlan {
    public:
      LogicalPlan* child;

      explicit LogicalDistinct(LogicalPlan* child);
      ~LogicalDistinct() override;
      PhysicalPlan::PhysicalDistinct* ToPhysical(CompileResult& context)override;
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
      PhysicalPlan::PhysicalInsert* ToPhysical(CompileResult& context)override;
  };

  class LogicalSchemaCreate final : public LogicalPlan {
    public:
      std::string schemaName;
      Int databaseId;
      explicit LogicalSchemaCreate(const DataTypes::Guid& sessionId, Int databaseId, std::string& schemaName);
      PhysicalPlan::PhysicalSchemaCreate* ToPhysical(CompileResult& context)override;
  };

  class LogicalDelete final : public LogicalPlan {
  public:
    Statements::DataSource* table;
    Expressions::Expression* expression;
    explicit LogicalDelete(Statements::DataSource* table, Expressions::Expression* expression);
    PhysicalPlan::ExecutionNode* ToPhysical(CompileResult& context)override;
  };

    class LogicalUpdate final : public LogicalPlan {
    public:
        Statements::DataSource* table;
        std::vector<Expressions::Expression*> updates;
        Expressions::Expression* expression;

        explicit LogicalUpdate(
          Statements::DataSource* table,
          std::vector<Expressions::Expression*>& updates,
          Expressions::Expression* expression
        );
        PhysicalPlan::ExecutionNode* ToPhysical(CompileResult& context)override;
    };

  class LogicalTableCreate final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      std::string constraintName;
      std::vector<Statements::NewColumn*> columns;
      std::vector<column_index_t> primaryKey;

      explicit LogicalTableCreate(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        std::vector<Statements::NewColumn*>& columns,
        std::vector<column_index_t> primaryKey,
        std::string  constraintName);
      PhysicalPlan::PhysicalTableCreate* ToPhysical(CompileResult& context)override;
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
    PhysicalPlan::ExecutionNode * ToPhysical(CompileResult& context) override;
  };

  class LogicalAlterTable final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      Constants::AlterTableType type;

      union {
        Statements::NewColumn* addColumn;
        Statements::AlterColumn* alterColumn;
        Statements::RenameColumn* renameColumn;
        Statements::DropColumn* dropColumn;
      } column;

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const AlterTableType& type,
        Statements::NewColumn* column
      );

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const AlterTableType& type,
        Statements::AlterColumn* column
      );

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const AlterTableType& type,
        Statements::RenameColumn* column
      );

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const AlterTableType& type,
        Statements::DropColumn* column
      );

      PhysicalPlan::ExecutionNode * ToPhysical(CompileResult& context) override;
  };
}

