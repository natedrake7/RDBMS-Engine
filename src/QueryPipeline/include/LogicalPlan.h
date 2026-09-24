#pragma once
#include "PhysicalPlan.h"

namespace QueryPipeline {
    struct JoinAlgorithmAnalysisResult;

    class LogicalPlan {
    protected:
        [[nodiscard]] static const CoreEngine::OutputSchema* BuildSchema(
            QueryContext& context,
            const DataStructures::PolymorphicArray<Expressions::Expression*>& array,
            const CoreEngine::OutputSchema* childSchema
        );
    public:
        session_id_t sessionId;
        Int databaseId;

        LogicalPlan(session_id_t sessionId, Int databaseId);
        explicit LogicalPlan(session_id_t sessionId);
        LogicalPlan();
        virtual ~LogicalPlan() = default;
        virtual PhysicalPlan::PlanNode* ToPhysical(QueryContext& context) = 0;
    };

    class LogicalMaterialize : public LogicalPlan{
        LogicalPlan* child;
        UnsignedSmallInt _slotIndex;

    public:
        LogicalMaterialize(
            LogicalPlan* child,
            UnsignedSmallInt slotIndex
        );
        [[nodiscard]] [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context) override;
    };

    class LogicalDeclareVariable final : public LogicalPlan {
    public:
        Variable variable;
        Expressions::Expression* expression;

        LogicalDeclareVariable(session_id_t sessionId, Variable& variable, Expressions::Expression* expression);
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context) override;
    };

    class LogicalCreateUser final : public LogicalPlan {
    public:
        DataTypes::String username;
        DataTypes::String password;
        DataTypes::String role;

        explicit LogicalCreateUser(
            session_id_t sessionId,
            DataTypes::String&  username,
            DataTypes::String& password,
            DataTypes::String& role
        );
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context) override;
    };

    class LogicalGrantRole final: public LogicalPlan {
    public:
        DataTypes::String username;
        DataTypes::String role;

        explicit LogicalGrantRole(session_id_t sessionId, DataTypes::String & username, DataTypes::String & role);
        ~LogicalGrantRole()override = default;
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context) override;
    };

    class LogicalCreateDatabase final : public LogicalPlan {
    public:
        DataTypes::String dbName;
        explicit LogicalCreateDatabase(session_id_t sessionId, DataTypes::String& dbName);
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
    };

    class LogicalUseDatabase final : public LogicalPlan {
    public:
        Int databaseId;
        session_id_t sessionId;

        explicit LogicalUseDatabase(session_id_t sessionId, Int databaseId);
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
    };

    class LogicalProject final: public LogicalPlan {
    public:
        LogicalPlan* child;
        DataStructures::PolymorphicArray<Expressions::Expression*> _projections;
        UnsignedSmallInt _slotCount;

        LogicalProject(
            LogicalPlan* child,
            DataStructures::PolymorphicArray<Expressions::Expression*>& projections,
            UnsignedSmallInt slotCount
        );
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
    };

    class LogicalTableScan final : public LogicalPlan {
        [[nodiscard]] bool HasPredicate()const;
        DataStructures::PolymorphicArray<CoreEngine::StorageTypes::FilterColumnInfo> BuildFilterColumns(
            const QueryContext& context,
            const CoreEngine::OutputSchema* schema
        ) const;

    public:
        Statements::DataSource* table;
        Expressions::Expression* expression;

        explicit LogicalTableScan(
            Statements::DataSource* table,
            Expressions::Expression* expression
        );
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context) override;
    };

  class LogicalJoin final : public LogicalPlan {
    static PhysicalPlan::PlanNode* CreateInnerJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    );
    static PhysicalPlan::PlanNode* CreateLeftJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    );
    static PhysicalPlan::PlanNode* CreateRightJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    );
    static PhysicalPlan::PlanNode* CreateFullJoinPhysicalPlan(
        const QueryContext& context,
        JoinAlgorithmAnalysisResult& analysis,
        PhysicalPlan::PlanNode* leftPlan,
        PhysicalPlan::PlanNode* rightPlan
    );

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

    [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
  };

    class LogicalFilter final : public LogicalPlan {
    public:
        LogicalPlan* child;
        Expressions::Expression* filter;
        UnsignedSmallInt _slotCount;

        explicit LogicalFilter( LogicalPlan* child, Expressions::Expression* filter, UnsignedSmallInt slotCount);
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
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
  };

  class LogicalTop final : public LogicalPlan {
    public:
      LogicalPlan* child;
      BigInt top;

      explicit LogicalTop(LogicalPlan* child, BigInt top);
      PhysicalPlan::PhysicalTop* ToPhysical(QueryContext& context)override;
  };

  class LogicalDistinct final : public LogicalPlan {
    public:
      LogicalPlan* child;

      explicit LogicalDistinct(LogicalPlan* child);
      PhysicalPlan::PhysicalDistinct* ToPhysical(QueryContext& context)override;
  };

    class LogicalInsert final : public LogicalPlan {
    public:
        CoreEngine::StorageTypes::InsertPlan insertPlan;
        Statements::DataSource* table;
        LogicalPlan* child;

        explicit LogicalInsert(
            Statements::DataSource* table,
            LogicalPlan* child,
            CoreEngine::StorageTypes::InsertPlan& insertPlan
        );
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
    };

    class LogicalValues final : public LogicalPlan{
    public:
        DataStructures::PolymorphicArray<Statements::Inserts> _values;
        DataStructures::PolymorphicArray<DataType> _valueTypes;

        explicit LogicalValues(
            DataStructures::PolymorphicArray<Statements::Inserts>& values,
            DataStructures::PolymorphicArray<DataType>& valueTypes
        );

        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
    };

    class LogicalSchemaCreate final : public LogicalPlan {
    public:
        DataTypes::String schemaName;
        Int databaseId;
        explicit LogicalSchemaCreate(session_id_t sessionId, Int databaseId, DataTypes::String& schemaName);
        PhysicalPlan::PhysicalSchemaCreate* ToPhysical(QueryContext& context)override;
    };

    class LogicalDelete final : public LogicalPlan {
    public:
        Statements::DataSource* table;
        Expressions::Expression* expression;
        explicit LogicalDelete(Statements::DataSource* table, Expressions::Expression* expression);
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
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
        [[nodiscard]] PhysicalPlan::PlanNode* ToPhysical(QueryContext& context)override;
    };

    class LogicalTableCreate final : public LogicalPlan {
    public:
        Statements::DataSource* table;
        DataTypes::String constraintName;
        DataStructures::PolymorphicArray<Statements::NewColumn*> columns;
        DataStructures::PolymorphicArray<column_index_t> primaryKey;

        explicit LogicalTableCreate(
            session_id_t sessionId,
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
            session_id_t sessionId,
            Statements::DataSource* table,
            DataTypes::String& constraintName,
            DataStructures::PolymorphicArray<column_index_t>& columns
        );
        PhysicalPlan::PlanNode* ToPhysical(QueryContext& context) override;
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
            session_id_t sessionId,
            Statements::DataSource* table,
            const Constants::AlterTableType& type,
            Statements::NewColumn* column
        );

        explicit LogicalAlterTable(
            session_id_t sessionId,
            Statements::DataSource* table,
            const Constants::AlterTableType& type,
            Statements::AlterColumn* column
        );

        explicit LogicalAlterTable(
            session_id_t sessionId,
            Statements::DataSource* table,
            const Constants::AlterTableType& type,
            Statements::RenameColumn* column
        );

        explicit LogicalAlterTable(
            session_id_t sessionId,
            Statements::DataSource* table,
            const Constants::AlterTableType& type,
            Statements::DropColumn* column
        );

        PhysicalPlan::PlanNode * ToPhysical(QueryContext& context) override;
    };
}

