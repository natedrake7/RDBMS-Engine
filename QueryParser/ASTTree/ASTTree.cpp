#include "ASTTree.h"
#include "../Tokenizer/Tokenizer.h"
#include <exception>

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

        KeyWord keywordEnumeration;
        if(!keywordsDictionary.TryGetValue(tokens[i].value, keywordEnumeration))
            throw exception("ASTTree::BuildTree::Invalid Query");

        //function based on keyword

        if(tokens[i].type == WordType::Keyword)
        {
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
                    break;
            
            }
        }

        if(i == tokens.size())
            return;
        
        ASTNode* newNode = new ASTNode();
        node->children.push_back(newNode);
        AstTree::BuildNode(newNode, tokens, i);

        return;


        if (tokens[i].value == "FROM") 
        {
            if(tokens[++i].value == "(")
            {
                ASTNode* subQueryNode = new ASTNode();
                node->children.push_back(subQueryNode);
                AstTree::BuildNode(subQueryNode, tokens, ++i);
            }
            else
                node->table = tokens[i].value;

            //skip closing bracket or table
            i++;
        }
    
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

    void AstTree::BuildSelectNode(ASTNode*& node, vector<Token>& tokens, int& depth)
    {
        node->type = KeyWord::Select;

        depth++;
        
        //get columns or constants
        while (tokens[depth].value != "FROM") 
        {
            if (tokens[depth].value != ",") 
                node->columns.push_back(tokens[depth].value);

            //add check for instead of columns to use
            // if(tokens[depth].value.contains("@"))
            // {
            //     node->columns.push_back(tokens[depth].value);

            //     if(tokens[++depth].type == WordType::Symbol)
            //     {

            //     }
            // }

            depth++;

            if(depth == tokens.size())
                return;
        }

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
                node->table = tokens[depth].value;

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