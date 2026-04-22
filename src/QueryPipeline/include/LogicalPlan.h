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
      virtual PhysicalPlan::ExecutionNode* ToPhysical(QueryContext& context) = 0;
  };

  class LogicalDeclareVariable final : public LogicalPlan {
    public:
      Variable variable;
      Expressions::Expression* expression;

      LogicalDeclareVariable(const DataTypes::Guid& sessionId, Variable& variable, Expressions::Expression* expression);
      PhysicalPlan::ExecutionNode * ToPhysical(QueryContext& context) override;
  };

  class LogicalCreateUser final : public LogicalPlan {
    public:
      DataTypes::String username;
      DataTypes::String password;
      DataTypes::String role;

    explicit LogicalCreateUser(
      const DataTypes::Guid& sessionId,
      DataTypes::String&  username,
      DataTypes::String& password,
      DataTypes::String& role
    );
    ~LogicalCreateUser()override;
    PhysicalPlan::ExecutionNode * ToPhysical(QueryContext& context) override;
  };

  class LogicalGrantRole final: public LogicalPlan {
    public:
      DataTypes::String username;
      DataTypes::String role;

    explicit LogicalGrantRole(const DataTypes::Guid& sessionId, DataTypes::String & username, DataTypes::String & role);
    ~LogicalGrantRole()override = default;
    PhysicalPlan::ExecutionNode * ToPhysical(QueryContext& context) override;
  };

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      DataTypes::String dbName;
      explicit LogicalCreateDatabase(const DataTypes::Guid& sessionId, DataTypes::String& dbName);
      PhysicalPlan::PhysicalCreateDatabase* ToPhysical(QueryContext& context)override;
  };

  class LogicalUseDatabase final : public LogicalPlan {
    public:
      Int databaseId;
      DataTypes::Guid sessionId;

      explicit LogicalUseDatabase(const DataTypes::Guid& sessionId, Int databaseId);
      PhysicalPlan::PhysicalUseDatabase* ToPhysical(QueryContext& context)override;
  };

  class LogicalProject final: public LogicalPlan {
    public:
      LogicalPlan* child;
      DataStructures::PolymorphicArray<Expressions::Expression*> resultExpressions;
      DataStructures::PolymorphicArray<Headers::ColumnHeader> columnsHeaders;

      LogicalProject(
        LogicalPlan* child,
        DataStructures::PolymorphicArray<Expressions::Expression*>& resultExpressions,
        DataStructures::PolymorphicArray<Headers::ColumnHeader>& columnsHeaders);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical(QueryContext& context)override;
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
      PhysicalPlan::ExecutionNode* ToPhysical(QueryContext& context) override;
  };

  class LogicalJoin final : public LogicalPlan {
    PhysicalPlan::ExecutionNode* CreateInnerJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const;
    PhysicalPlan::ExecutionNode* CreateLeftJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const;
    PhysicalPlan::ExecutionNode* CreateRightJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const;
    PhysicalPlan::ExecutionNode* CreateFullJoinPhysicalPlan(QueryContext& context, JoinAlgorithmAnalysisResult& analysis) const;

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

    PhysicalPlan::ExecutionNode* ToPhysical(QueryContext& context)override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Expressions::Expression* filter;

      explicit LogicalFilter( LogicalPlan* child, Expressions::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical(QueryContext& context)override;
  };

  class LogicalOrder final : public LogicalPlan {
    public:
      LogicalPlan* child;
      DataStructures::PolymorphicArray<Statements::OrderColumn*> expressions;

      explicit LogicalOrder(
        LogicalPlan* child,
        DataStructures::PolymorphicArray<Statements::OrderColumn*>& expressions
      );
      PhysicalPlan::ExecutionNode* ToPhysical(QueryContext& context)override;
  };

  class LogicalTop final : public LogicalPlan {
    public:
      LogicalPlan* child;
      BigInt top;

      explicit LogicalTop(LogicalPlan* child, BigInt top);
      ~LogicalTop() override;
      PhysicalPlan::PhysicalTop* ToPhysical(QueryContext& context)override;
  };

  class LogicalDistinct final : public LogicalPlan {
    public:
      LogicalPlan* child;

      explicit LogicalDistinct(LogicalPlan* child);
      ~LogicalDistinct() override;
      PhysicalPlan::PhysicalDistinct* ToPhysical(QueryContext& context)override;
  };

  class LogicalInsert final : public LogicalPlan {
    public:
      Statements::DataSource* table;
      DataStructures::PolymorphicArray<Statements::Inserts> fields;

      LogicalPlan* child;
      DataStructures::PolymorphicArray<column_index_t> columnsIndices;

      explicit LogicalInsert(
        Statements::DataSource* table,
        DataStructures::PolymorphicArray<Statements::Inserts>& fields,
        LogicalPlan* child,
        DataStructures::PolymorphicArray<column_index_t>& columnIndices
      );
      ~LogicalInsert()override;
      PhysicalPlan::PhysicalInsert* ToPhysical(QueryContext& context)override;
  };

  class LogicalSchemaCreate final : public LogicalPlan {
    public:
      DataTypes::String schemaName;
      Int databaseId;
      explicit LogicalSchemaCreate(const DataTypes::Guid& sessionId, Int databaseId, DataTypes::String& schemaName);
      PhysicalPlan::PhysicalSchemaCreate* ToPhysical(QueryContext& context)override;
  };

  class LogicalDelete final : public LogicalPlan {
  public:
    Statements::DataSource* table;
    Expressions::Expression* expression;
    explicit LogicalDelete(Statements::DataSource* table, Expressions::Expression* expression);
    PhysicalPlan::ExecutionNode* ToPhysical(QueryContext& context)override;
  };

    class LogicalUpdate final : public LogicalPlan {
    public:
        Statements::DataSource* table;
        DataStructures::PolymorphicArray<Expressions::Expression*> updates;
        Expressions::Expression* expression;

        explicit LogicalUpdate(
          Statements::DataSource* table,
          DataStructures::PolymorphicArray<Expressions::Expression*>& updates,
          Expressions::Expression* expression
        );
        PhysicalPlan::ExecutionNode* ToPhysical(QueryContext& context)override;
    };

    class LogicalTableCreate final : public LogicalPlan {
    public:
        Statements::DataSource* table;
        DataTypes::String constraintName;
        DataStructures::PolymorphicArray<Statements::NewColumn*> columns;
        DataStructures::PolymorphicArray<column_index_t> primaryKey;

        explicit LogicalTableCreate(
            const DataTypes::Guid& sessionId,
            Statements::DataSource* table,
            DataStructures::PolymorphicArray<Statements::NewColumn*>& columns,
            DataStructures::PolymorphicArray<column_index_t> primaryKey,
            DataTypes::String& constraintName
        );
        PhysicalPlan::PhysicalTableCreate* ToPhysical(QueryContext& context)override;
    };

  class LogicalIndexCreate final : public LogicalPlan {
    public:
    Statements::DataSource* table;
    DataTypes::String constraintName;
    DataStructures::PolymorphicArray<column_index_t> columns;
    explicit LogicalIndexCreate(
      const DataTypes::Guid& sessionId,
      Statements::DataSource* table,
      DataTypes::String& constraintName,
      DataStructures::PolymorphicArray<column_index_t>& columns
    );
    PhysicalPlan::ExecutionNode * ToPhysical(QueryContext& context) override;
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
        const Constants::AlterTableType& type,
        Statements::NewColumn* column
      );

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const Constants::AlterTableType& type,
        Statements::AlterColumn* column
      );

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const Constants::AlterTableType& type,
        Statements::RenameColumn* column
      );

      explicit LogicalAlterTable(
        const DataTypes::Guid& sessionId,
        Statements::DataSource* table,
        const Constants::AlterTableType& type,
        Statements::DropColumn* column
      );

      PhysicalPlan::ExecutionNode * ToPhysical(QueryContext& context) override;
  };
}

