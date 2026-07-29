#include "../../include/Parsing/Token.h"

namespace QueryPipeline::Parsing{

    bool CanBeIdentifier(const TokenType type){
        if (type == TokenType::Identifier)
            return true;

        switch (type){
            //Mirrors the reservedAsIdentifier rule in SQL.g4.
            case TokenType::User:
            case TokenType::Table:
            case TokenType::Index:
            case TokenType::Create:
            case TokenType::Drop:
            case TokenType::Insert:
            case TokenType::Delete:
            case TokenType::Update:
            case TokenType::On:
            case TokenType::To:
            case TokenType::Top:
            case TokenType::Distinct:
            case TokenType::With:
            case TokenType::Password:
            case TokenType::Role:
            case TokenType::Grant:
            case TokenType::Order:
            case TokenType::By:
            case TokenType::Values:
            case TokenType::Into:
            case TokenType::Database:
            case TokenType::Use:
            case TokenType::Schema:
            case TokenType::Declare:
            case TokenType::Set:
            case TokenType::Alter:
            case TokenType::Rename:
            case TokenType::Add:
            case TokenType::Column:
            case TokenType::Unique:
            case TokenType::Primary:
            case TokenType::Key:
            case TokenType::Identity:
            case TokenType::Constraint:
            case TokenType::Count:
            case TokenType::Sum:
            case TokenType::Avg:
            case TokenType::Min:
            case TokenType::Max:
            case TokenType::Desc:
            case TokenType::Asc:
            case TokenType::Left:
            case TokenType::Right:
            case TokenType::Full:
            case TokenType::Inner:
            case TokenType::Outer:
            case TokenType::Join:
            case TokenType::As:
            case TokenType::Bool:
            case TokenType::TinyInt:
            case TokenType::SmallInt:
            case TokenType::Int:
            case TokenType::BigInt:
            case TokenType::Decimal:
            case TokenType::DateTime:
            case TokenType::Guid:
            case TokenType::Json:
            case TokenType::String:
            //WHEN is lexed for the sake of the SWITCH family but no rule consumes it,
            //so it stays usable as a name.
            case TokenType::When:
                return true;
            default:
                return false;
        }
    }

    DataTypes::StringView TokenTypeName(const TokenType type){
        switch (type){
            case TokenType::EndOfFile:              return DataTypes::StringView("end of input");
            case TokenType::Invalid:                return DataTypes::StringView("invalid token");

            case TokenType::Identifier:             return DataTypes::StringView("identifier");
            case TokenType::Variable:               return DataTypes::StringView("variable");
            case TokenType::IntegerLiteral:         return DataTypes::StringView("integer literal");
            case TokenType::DecimalLiteral:         return DataTypes::StringView("decimal literal");
            case TokenType::StringLiteral:          return DataTypes::StringView("string literal");
            case TokenType::UnicodeStringLiteral:   return DataTypes::StringView("unicode string literal");

            case TokenType::LeftParen:              return DataTypes::StringView("(");
            case TokenType::RightParen:             return DataTypes::StringView(")");
            case TokenType::Comma:                  return DataTypes::StringView(",");
            case TokenType::Semicolon:              return DataTypes::StringView(";");
            case TokenType::Dot:                    return DataTypes::StringView(".");

            case TokenType::Plus:                   return DataTypes::StringView("+");
            case TokenType::Minus:                  return DataTypes::StringView("-");
            case TokenType::Star:                   return DataTypes::StringView("*");
            case TokenType::Slash:                  return DataTypes::StringView("/");
            case TokenType::Percent:                return DataTypes::StringView("%");

            case TokenType::Equal:                  return DataTypes::StringView("=");
            case TokenType::NotEqual:               return DataTypes::StringView("!=");
            case TokenType::Less:                   return DataTypes::StringView("<");
            case TokenType::LessEqual:              return DataTypes::StringView("<=");
            case TokenType::Greater:                return DataTypes::StringView(">");
            case TokenType::GreaterEqual:           return DataTypes::StringView(">=");

            case TokenType::JsonObjectAccessor:     return DataTypes::StringView("->");
            case TokenType::JsonScalarAccessor:     return DataTypes::StringView("->>");

            case TokenType::Select:                 return DataTypes::StringView("SELECT");
            case TokenType::From:                   return DataTypes::StringView("FROM");
            case TokenType::Where:                  return DataTypes::StringView("WHERE");
            case TokenType::Order:                  return DataTypes::StringView("ORDER");
            case TokenType::By:                     return DataTypes::StringView("BY");
            case TokenType::Values:                 return DataTypes::StringView("VALUES");
            case TokenType::Into:                   return DataTypes::StringView("INTO");
            case TokenType::Database:               return DataTypes::StringView("DATABASE");
            case TokenType::Use:                    return DataTypes::StringView("USE");
            case TokenType::Create:                 return DataTypes::StringView("CREATE");
            case TokenType::Drop:                   return DataTypes::StringView("DROP");
            case TokenType::Insert:                 return DataTypes::StringView("INSERT");
            case TokenType::Delete:                 return DataTypes::StringView("DELETE");
            case TokenType::Update:                 return DataTypes::StringView("UPDATE");
            case TokenType::On:                     return DataTypes::StringView("ON");
            case TokenType::Table:                  return DataTypes::StringView("TABLE");
            case TokenType::Default:                return DataTypes::StringView("DEFAULT");
            case TokenType::To:                     return DataTypes::StringView("TO");
            case TokenType::Top:                    return DataTypes::StringView("TOP");
            case TokenType::Distinct:               return DataTypes::StringView("DISTINCT");
            case TokenType::With:                   return DataTypes::StringView("WITH");
            case TokenType::Password:               return DataTypes::StringView("PASSWORD");
            case TokenType::Role:                   return DataTypes::StringView("ROLE");
            case TokenType::Grant:                  return DataTypes::StringView("GRANT");
            case TokenType::User:                   return DataTypes::StringView("USER");
            case TokenType::Schema:                 return DataTypes::StringView("SCHEMA");

            case TokenType::Declare:                return DataTypes::StringView("DECLARE");
            case TokenType::Set:                    return DataTypes::StringView("SET");

            case TokenType::Alter:                  return DataTypes::StringView("ALTER");
            case TokenType::Rename:                 return DataTypes::StringView("RENAME");
            case TokenType::Add:                    return DataTypes::StringView("ADD");
            case TokenType::Column:                 return DataTypes::StringView("COLUMN");

            case TokenType::Unique:                 return DataTypes::StringView("UNIQUE");
            case TokenType::Index:                  return DataTypes::StringView("INDEX");
            case TokenType::Primary:                return DataTypes::StringView("PRIMARY");
            case TokenType::Key:                    return DataTypes::StringView("KEY");
            case TokenType::Identity:               return DataTypes::StringView("IDENTITY");
            case TokenType::Constraint:             return DataTypes::StringView("CONSTRAINT");

            case TokenType::Count:                  return DataTypes::StringView("COUNT");
            case TokenType::Sum:                    return DataTypes::StringView("SUM");
            case TokenType::Avg:                    return DataTypes::StringView("AVG");
            case TokenType::Min:                    return DataTypes::StringView("MIN");
            case TokenType::Max:                    return DataTypes::StringView("MAX");

            case TokenType::Not:                    return DataTypes::StringView("NOT");
            case TokenType::Null:                   return DataTypes::StringView("NULL");

            case TokenType::Desc:                   return DataTypes::StringView("DESC");
            case TokenType::Asc:                    return DataTypes::StringView("ASC");

            case TokenType::Left:                   return DataTypes::StringView("LEFT");
            case TokenType::Right:                  return DataTypes::StringView("RIGHT");
            case TokenType::Full:                   return DataTypes::StringView("FULL");
            case TokenType::Inner:                  return DataTypes::StringView("INNER");
            case TokenType::Outer:                  return DataTypes::StringView("OUTER");
            case TokenType::Join:                   return DataTypes::StringView("JOIN");

            case TokenType::As:                     return DataTypes::StringView("AS");

            case TokenType::And:                    return DataTypes::StringView("AND");
            case TokenType::Or:                     return DataTypes::StringView("OR");

            case TokenType::Switch:                 return DataTypes::StringView("SWITCH");
            case TokenType::Case:                   return DataTypes::StringView("CASE");
            case TokenType::When:                   return DataTypes::StringView("WHEN");
            case TokenType::Then:                   return DataTypes::StringView("THEN");
            case TokenType::Iif:                    return DataTypes::StringView("IIF");

            case TokenType::Cast:                   return DataTypes::StringView("CAST");
            case TokenType::TryCast:                return DataTypes::StringView("TRY_CAST");

            case TokenType::True:                   return DataTypes::StringView("TRUE");
            case TokenType::False:                  return DataTypes::StringView("FALSE");

            case TokenType::Bool:                   return DataTypes::StringView("BOOL");
            case TokenType::TinyInt:                return DataTypes::StringView("TINYINT");
            case TokenType::SmallInt:               return DataTypes::StringView("SMALLINT");
            case TokenType::Int:                    return DataTypes::StringView("INT");
            case TokenType::BigInt:                 return DataTypes::StringView("BIGINT");
            case TokenType::Decimal:                return DataTypes::StringView("DECIMAL");
            case TokenType::DateTime:               return DataTypes::StringView("DATETIME");
            case TokenType::Guid:                   return DataTypes::StringView("GUID");
            case TokenType::Json:                   return DataTypes::StringView("JSON");
            case TokenType::String:                 return DataTypes::StringView("STRING");
        }

        return "unknown token";
    }
}
