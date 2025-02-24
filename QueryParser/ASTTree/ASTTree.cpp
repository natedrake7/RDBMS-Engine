#include "ASTTree.h"
#include "../Tokenizer/Tokenizer.h"
#include <exception>
#include <stdexcept>
#include <vector>

namespace QueryParser 
{
    ASTNode::ASTNode() = default;

    ASTNode::~ASTNode() 
    {
        for (auto child : children) 
            delete child;
    }

    AstTree::AstTree() : root(nullptr) {}
    
    AstTree::~AstTree()
    {
        delete root;
    };

    bool AstTree::IsNumber(const string& str)
    {
        try 
        {
            const auto res = std::stod(str);
            return true;
        } 
        catch (...) 
        {
            return false;
        }
    }

    void AstTree::InitializeColumnOperations(vector<SelectColumn>& columns, vector<SelectColumn>& operations, Token& alias)
    {
        columns.clear();
        operations.clear();
        alias.type = WordType::Uknown;
        alias.value = "";
    }

    ASTNode* AstTree::BuildTree(vector<Token>& tokens)
    {
        int startingDepth = 0;
        this->root = nullptr;

        AstTree::BuildNode(this->root, tokens, startingDepth);

        return this->root;
    }
    

    void AstTree::BuildNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth) 
    {
        if(node == nullptr)
            node = new ASTNode();

        int i = startingDepth;
        //each node should start with a KeyWord
        //function based on keyword
        if(tokens[i].type != WordType::Keyword)
            throw runtime_error("AstTree::BuildNode: Invalid Query");

        KeyWord keywordEnumeration;
        if(!keywordsDictionary.TryGetValue(tokens[i].value, keywordEnumeration))
            throw exception("ASTTree::BuildNode: Invalid Query");

        //build starting node by datatype
        switch (keywordEnumeration)
        {
            case KeyWord::Select:
                AstTree::BuildSelectNode(node, tokens, i);
                break;
            case KeyWord::Insert:
                AstTree::BuildInsertNode(node, tokens, i);
                break;
            case KeyWord::Update:
                AstTree::BuildUpdateNode(node, tokens, i);
                break;
            case KeyWord::Delete:
                AstTree::BuildDeleteNode(node, tokens, i);
                break;
            default:
               throw runtime_error("ASTTree::BuildNode: Invalid Keyword specified");        
        }

        if(i == tokens.size())
            return;
        
        ASTNode* newNode = new ASTNode();
        node->children.push_back(newNode);
        AstTree::BuildNode(newNode, tokens, i);

        return;

        if (tokens[i].value == "WHERE") 
        {
            ASTNode* whereNode = new ASTNode();
            //handle more complex queries like Subqueries
            whereNode->whereClause.column = tokens[++i].value;
            whereNode->whereClause.op = tokens[++i].value;
            whereNode->whereClause.value = tokens[++i].value;
            node->children.push_back(whereNode);
            i++;
        }

        if (i < tokens.size() && tokens[i].value == "GROUP" && tokens[i + 1].value == "BY") 
        {
            i += 2;  // Skip "GROUP BY"
            ASTNode* groupByNode = new ASTNode();

            while(i < tokens.size() && ( tokens[i].value != "HAVING" && tokens[i].value != ";" && tokens[i].value != "ORDER"))
                groupByNode->groupBy.columns.push_back(tokens[i++].value);

            if(tokens[i].value == "HAVING")
            {
                groupByNode->groupBy.having = new ASTNode();
                groupByNode->groupBy.having->whereClause.column = tokens[++i].value;
                groupByNode->groupBy.having->whereClause.op = tokens[++i].value;
                groupByNode->groupBy.having->whereClause.value = tokens[++i].value;
                
            }

            node->children.push_back(groupByNode);
            i++;       
        }
    
        if (i < tokens.size() && tokens[i].value == "ORDER" && tokens[i + 1].value == "BY") 
        {
            i += 2;  // Skip "ORDER BY"
            ASTNode* orderByNode = new ASTNode();
            orderByNode->orderBy.column = tokens[i++].value;
            
            orderByNode->orderBy.direction = tokens[i++].value; // ASC or DESC
            node->children.push_back(orderByNode);
        }

        startingDepth = i;
    }

    void AstTree:: BuildSelectNode(ASTNode*& node, vector<Token>& tokens, int& depth)
    {
        node->type = KeyWord::Select;

        depth++;
        
        //get columns or constants
        int i = 0;
        vector<SelectColumn> columns;
        vector<SelectColumn> operations;
        Token alias;

        while (tokens[depth].value != "FROM") 
        {
            if (tokens[depth].value == ",") 
            {
                node->columns.push_back({.columns = columns, .operation = operations, .alias = alias});
                i++;
                depth++;
                AstTree::InitializeColumnOperations(columns, operations, alias);
                continue;
            }

            switch (tokens[depth].type) 
            {
                case WordType::Symbol:
                    operations.push_back({tokens[depth]});
                    break;
                case WordType::Keyword:
                    alias = tokens[++depth];
                    break;
                case WordType::Identifier:
                case WordType::Number:
                case WordType::String:
                case WordType::WildCard:
                case WordType::ScalarVariable:
                    columns.push_back({tokens[depth]});
                    break;
                case WordType::AggregateFunction:
                    columns.push_back({tokens[depth], tokens[++depth]});
                    break;
                case WordType::Uknown:
                default:
                    throw runtime_error("Invalid Query");
            }

            depth++;

            if(depth == tokens.size())
            {
                node->columns.push_back({.columns = columns, .operation = operations, .alias = alias});
                return;
            }
        }

        if(columns.empty())
            throw runtime_error("Invalid Query");

        node->columns.push_back({.columns = columns, .operation = operations, .alias = alias});

        //get table or subquery
        if (tokens[depth].value == "FROM") 
        {
            if(tokens[++depth].value == "(")
            {
                ASTNode* subQueryNode = new ASTNode();
                node->children.push_back(subQueryNode);

                AstTree::BuildNode(subQueryNode, tokens, ++depth);
            }
            else
                node->table = tokens[depth];

            //skip closing bracket or table
            depth++;
        }
    }

    void AstTree::BuildInsertNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth)
    {

    }

    void AstTree::BuildUpdateNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth)
    {

    }
    
    void AstTree::BuildDeleteNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth)
    {

    }
}