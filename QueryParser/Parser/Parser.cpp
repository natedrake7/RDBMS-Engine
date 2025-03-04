#include "Parser.h"
#include "../Tokenizer/Tokenizer.h"
#include <stdexcept>
#include <string>
#include <vector>

namespace QueryParser 
{
    Parser::Parser() = default;

    Parser::~Parser()
    {
        for(const auto& statement : this->query.statements)
        {
            for(auto& node : statement->nodes)
                delete node;
        }
    }

    Node::Node() = default;

    Node::~Node()
    {
        for(auto& child: children)
            delete child;
    }

    Query::Query()
    {
        this->type = WordType::Program;
    }
    
    Query::~Query()
    {
        for(auto& statement: statements)
            delete statement;
    }

    Statement::~Statement()
    {
        for(auto& node : nodes)
            delete node;
    }

    std::ostream& operator<<(std::ostream& os, const Node& node) 
    {
        os  << "Node(Type: " 
            << wordTypeLiterals.Get(node.type) 
            << ", Value: ";

        // Use std::visit to print the correct value from the std::variant
        std::visit([&os](const auto& val) 
        {
            os << val;
        }, node.value);

        os << ")";
        return os;
    }

    Query& Parser::Parse(vector<Token>& tokens)
    {
        if(!tokens.empty())
        {
            Statement* firstStatement = new Statement();
            this->query.statements.push_back(firstStatement);
        }

        for(const auto& token: tokens)
        {
            Node* node = nullptr;
            switch (token.type) 
            {
                case WordType::Number:
                    node = Parser::ParseNumber(token);
                    break;
                case WordType::String:
                    node = Parser::ParseString(token);
                    break;
                case WordType::LeftParenthesis:
                {
                    BlockStatement* statement = new BlockStatement();
                    this->query.statements.push_back(statement);
                    break;
                }
                case WordType::RightParenthesis:
                {
                    const auto iterator = this->query.statements.back();
                    if(dynamic_cast<BlockStatement*>(iterator) == nullptr)
                        throw runtime_error("Enclosing parenthesis not specified");
                    break;
                }
                case WordType::Semicolon:
                {
                    Statement* statement = new Statement();
                    this->query.statements.push_back(statement);
                    break;
                }
                default:
                    throw runtime_error("Invalid token type");
            }

            if(node == nullptr)
                continue;
            
            this->query.statements.back()->nodes.push_back(node);
        }

        return this->query;
    }

    Node* Parser::ParseNumber(const Token& token)
    {
        Node* node = new Node();

        node->type = token.type;
        node->value = stoll(token.value);

        return node;
    }

    Node* Parser::ParseString(const Token& token)
    {        
        Node* node = new Node();

        node->type = token.type;
        node->value = token.value;

        return node;
    }

    // ASTNode::ASTNode() = default;

    // ASTNode::~ASTNode() 
    // {
    //     for (auto child : children) 
    //         delete child;
    // }

    // AstTree::AstTree() : root(nullptr) {}
    
    // AstTree::~AstTree()
    // {
    //     delete root;
    // };

    // bool AstTree::IsNumber(const string& str)
    // {
    //     try 
    //     {
    //         const auto res = std::stod(str);
    //         return true;
    //     } 
    //     catch (...) 
    //     {
    //         return false;
    //     }
    // }

    // void AstTree::InitializeColumnOperations(vector<SelectColumn>& columns, vector<SelectColumn>& operations, Token& alias)
    // {
    //     columns.clear();
    //     operations.clear();
    //     alias.type = WordType::Uknown;
    //     alias.value = "";
    // }

    // ASTNode* AstTree::BuildTree(vector<Token>& tokens)
    // {
    //     int startingDepth = 0;
    //     this->root = nullptr;
    //     this->SearchQueryForJoinOperations(tokens);
        
    //     AstTree::BuildNode(this->root, tokens, startingDepth);

    //     return this->root;
    // }
    

    // void AstTree::BuildNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth) 
    // {
    //     if(node == nullptr)
    //         node = new ASTNode();

    //     int i = startingDepth;
    //     //each node should start with a KeyWord
    //     //function based on keyword
    //     if(tokens[i].type != WordType::Keyword)
    //         throw runtime_error("AstTree::BuildNode: Invalid Query");

    //     KeyWord keywordEnumeration;
    //     if(!keywordsDictionary.TryGetValue(tokens[i].value, keywordEnumeration)
    //         && !joinsKeywordsDictionary.TryGetValue(tokens[i].value, keywordEnumeration))
    //         throw exception("ASTTree::BuildNode: Invalid Query");



    //     //build starting node by datatype
    //     switch (keywordEnumeration)
    //     {
    //         case KeyWord::Select:
    //             AstTree::BuildSelectNode(node, tokens, i);
    //             break;
    //         case KeyWord::Insert:
    //             AstTree::BuildInsertNode(node, tokens, i);
    //             break;
    //         case KeyWord::Update:
    //             AstTree::BuildUpdateNode(node, tokens, i);
    //             break;
    //         case KeyWord::Delete:
    //             AstTree::BuildDeleteNode(node, tokens, i);
    //             break;
    //         case KeyWord::Inner:
    //         case KeyWord::Left:
    //         case KeyWord::Right:
    //         case KeyWord::Full:
    //         case KeyWord::Join:
    //             AstTree::BuildJoinNode(node, tokens, i);
    //             break;
    //         default:
    //            throw runtime_error("ASTTree::BuildNode: Invalid Keyword specified");        
    //     }

    //     if(i == tokens.size())
    //         return;
        
    //     ASTNode* newNode = new ASTNode();
    //     node->children.push_back(newNode);
    //     AstTree::BuildNode(newNode, tokens, i);

    //     return;

    //     // if (tokens[i].value == "WHERE") 
    //     // {
    //     //     ASTNode* whereNode = new ASTNode();
    //     //     //handle more complex queries like Subqueries
    //     //     whereNode->whereClause.column = tokens[++i].value;
    //     //     whereNode->whereClause.op = tokens[++i].value;
    //     //     whereNode->whereClause.value = tokens[++i].value;
    //     //     node->children.push_back(whereNode);
    //     //     i++;
    //     // }

    //     if (i < tokens.size() && tokens[i].value == "GROUP" && tokens[i + 1].value == "BY") 
    //     {
    //         i += 2;  // Skip "GROUP BY"
    //         ASTNode* groupByNode = new ASTNode();

    //         while(i < tokens.size() && ( tokens[i].value != "HAVING" && tokens[i].value != ";" && tokens[i].value != "ORDER"))
    //             groupByNode->groupBy.columns.push_back(tokens[i++].value);

    //         // if(tokens[i].value == "HAVING")
    //         // {
    //         //     groupByNode->groupBy.having = new ASTNode();
    //         //     groupByNode->groupBy.having->whereClause.column = tokens[++i].value;
    //         //     groupByNode->groupBy.having->whereClause.op = tokens[++i].value;
    //         //     groupByNode->groupBy.having->whereClause.value = tokens[++i].value;
                
    //         // }

    //         node->children.push_back(groupByNode);
    //         i++;       
    //     }
    
    //     if (i < tokens.size() && tokens[i].value == "ORDER" && tokens[i + 1].value == "BY") 
    //     {
    //         i += 2;  // Skip "ORDER BY"
    //         ASTNode* orderByNode = new ASTNode();
    //         orderByNode->orderBy.column = tokens[i++].value;
            
    //         orderByNode->orderBy.direction = tokens[i++].value; // ASC or DESC
    //         node->children.push_back(orderByNode);
    //     }

    //     startingDepth = i;
    // }

    // void AstTree:: BuildSelectNode(ASTNode*& node, vector<Token>& tokens, int& depth)
    // {
    //     node->type = KeyWord::Select;

    //     depth++;
        
    //     //get columns or constants
    //     int i = 0;
    //     vector<SelectColumn> columns;
    //     vector<SelectColumn> operations;
    //     Token alias;

    //     while (tokens[depth].value != "FROM") 
    //     {
    //         if (tokens[depth].value == ",") 
    //         {
    //             node->columns.push_back({.columns = columns, .operation = operations, .alias = alias});
    //             i++;
    //             depth++;
    //             AstTree::InitializeColumnOperations(columns, operations, alias);
    //             continue;
    //         }

    //         switch (tokens[depth].type) 
    //         {
    //             case WordType::Symbol:
    //                 operations.push_back({tokens[depth]});
    //                 break;
    //             case WordType::Keyword:
    //                 alias = tokens[++depth];
    //                 break;
    //             case WordType::Identifier:
    //             case WordType::Number:
    //             case WordType::String:
    //             case WordType::WildCard:
    //             case WordType::ScalarVariable:
    //                 columns.push_back({tokens[depth]});
    //                 break;
    //             case WordType::AggregateFunction:
    //                 columns.push_back({tokens[depth], tokens[++depth]});
    //                 break;
    //             case WordType::Uknown:
    //             default:
    //                 throw runtime_error("Invalid Query");
    //         }

    //         depth++;

    //         if(depth == tokens.size())
    //         {
    //             node->columns.push_back({.columns = columns, .operation = operations, .alias = alias});
    //             return;
    //         }
    //     }

    //     if(columns.empty())
    //         throw runtime_error("Invalid Query");

    //     node->columns.push_back({.columns = columns, .operation = operations, .alias = alias});

    //     //get table or subquery
    //     if (tokens[depth].value == "FROM") 
    //     {
    //         if(tokens[++depth].value == "(")
    //         {
    //             ASTNode* subQueryNode = new ASTNode();
    //             node->children.push_back(subQueryNode);

    //             AstTree::BuildNode(subQueryNode, tokens, ++depth);
    //         }
    //         else
    //             node->table = tokens[depth];

    //         //skip closing bracket or table
    //         depth++;
    //     }
    // }

    // WhereOperation AstTree::BuildJoinCondition(vector<Token>& tokens, int& depth)
    // {
    //     WhereOperation operation;

    //     if(depth + 3 >= tokens.size())
    //         throw std::runtime_error("Invalid Query");

    //     operation.leftOperand = tokens[++depth];
    //     operation.operation = tokens[++depth];
    //     operation.rightOperand = tokens[++depth];
    
    //     return operation;
    // }

    // void AstTree::SearchQueryForJoinOperations(vector<Token>& tokens)
    // {
    //     for (int i = 0; i < tokens.size(); i++)
    //     {
    //         KeyWord joinKeyword;
    //         const auto& firstToken = tokens[i];
    
    //         // Look for valid join types (e.g., INNER, LEFT, RIGHT, FULL)
    //         if (!joinTypeKeywordDictionary.TryGetValue(firstToken.value, joinKeyword))
    //             continue;
    //             // Ensure that we check for "JOIN" as the next token
    //         if (i + 1 >= tokens.size() || tokens[i + 1].value != "JOIN")
    //             continue;

    //         switch (joinKeyword)
    //         {
    //             case KeyWord::RightJoin:
    //                 this->RightJoinIndexes.push_back(i);
    //                 break;
    //             case KeyWord::LeftJoin:
    //                 this->LeftJoinIndexes.push_back(i);
    //                 break;
    //             case KeyWord::FullJoin:
    //                 this->FullJoinIndexes.push_back(i);
    //                 break;
    //             case KeyWord::InnerJoin:
    //                 this->InnerJoinIndexes.push_back(i);
    //                 break;
    //             default:
    //                 throw runtime_error("Invalid Query: Unknown Join Type");
    //         }
    //         i++; // Skip over the "JOIN" token
    //     }
    // }

    // void AstTree::BuildJoinNode(ASTNode*& node, vector<Token>& tokens, int& depth)
    // {
    //     string table;
    //     string tableAlias;

    //     const int innerJoinPos = -1;//AstTree::SearchForInnerJoins(tokens, depth);
    //     if(innerJoinPos != -1)
    //     {
    //         //handle inner join first
    //         ASTNode* newNode = new ASTNode();
    //         AstTree::BuildJoinNode(newNode, tokens, depth);
    //     }

    //     node->type = AstTree::SetAppropriateJoinKeyword(tokens, depth);
        
    //     while(tokens[depth].value != "ON")
    //     {
    //         switch (tokens[depth].type) 
    //         {
    //             case WordType::Keyword:
    //                 node->tableAlias = tokens[++depth];
    //                 depth++;
    //                 break;
    //             case WordType::Identifier:
    //                 node->table = tokens[depth++];
    //                 break;
    //             case WordType::Uknown:
    //             default:
    //                 throw runtime_error("Invalid Query");
    //         }
    //     }

    //     while(depth < tokens.size()
    //         && ( tokens[depth].value == "ON" 
    //         && ( tokens[depth].value == "ON" 
    //             || tokens[depth].value == "OR" 
    //             || tokens[depth].value == "AND")))
    //         {
    //             if(tokens[depth].value != "ON")
    //                 node->whereClause.operationCondition.push_back(tokens[depth]);
    //             node->whereClause.operations.push_back(AstTree::BuildJoinCondition(tokens, depth));
    //             if(tokens[depth].value != "ON")
    //                 node->whereClause.operationCondition.push_back(tokens[depth]);
    //             node->whereClause.operations.push_back(AstTree::BuildJoinCondition(tokens, depth));
    //             depth++;
    //         }
    // }

    // void AstTree::BuildInsertNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth)
    // {

    // }

    // void AstTree::BuildUpdateNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth)
    // {

    // }
    
    // void AstTree::BuildDeleteNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth)
    // {

    // }

    // KeyWord AstTree::SetAppropriateJoinKeyword(vector<Token>& tokens, int& depth)
    // {
    //     if (depth >= tokens.size())
    //         throw runtime_error("Invalid Query");

    //     const string& firstValue = tokens[depth].value;

    //     if(firstValue == "JOIN")
    //     {
    //         depth++;
    //         return KeyWord::InnerJoin;
    //     }

    //     if(depth + 1 >= tokens.size())
    //         throw runtime_error("Invalid Query");

    //     const string& secondValue = tokens[++depth].value;

    //     KeyWord joinKeyword;
    //     if (joinTypeKeywordDictionary.TryGetValue(firstValue, joinKeyword) 
    //         && secondValue == "JOIN")
    //     {
    //         depth++;
    //         return joinKeyword;
    //     }

    //     throw runtime_error("Invalid Query");
    // }

    // // KeyWord AstTree::SetAppropriateJoinKeyword(vector<Token>& tokens, int& depth)
    // // {
    // //     if (depth >= tokens.size())
    // //         throw runtime_error("Invalid Query");

    // //     const string& firstValue = tokens[depth].value;

    // //     if(firstValue == "JOIN")
    // //     {
    // //         depth++;
    // //         return KeyWord::InnerJoin;
    // //     }

    // //     if(depth + 1 >= tokens.size())
    // //         throw runtime_error("Invalid Query");

    // //     const string& secondValue = tokens[++depth].value;

    // //     KeyWord joinKeyword;
    // //     if (joinTypeKeywordDictionary.TryGetValue(firstValue, joinKeyword) 
    // //         && secondValue == "JOIN")
    // //     {
    // //         depth++;
    // //         return joinKeyword;
    // //     }

    // //     throw runtime_error("Invalid Query");
    // // }
}