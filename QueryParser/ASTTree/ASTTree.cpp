#include "ASTTree.h"
#include "../Tokenizer/Tokenizer.h"
#include <exception>

namespace QueryParser 
{
    ASTNode::ASTNode(string type) : type(type) {}

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
            node = new ASTNode("SELECT");

        int i = startingDepth;

        if(!keywordsHashSet.TryGetValue(tokens[i].value, node->type))
            throw exception("ASTTree::BuildTree::Invalid Query");

        //function based on keyword

        if(tokens[i].type == WordType::Keyword)
        {
            //build starting node by datatype
            switch (keywordsDictionary.Get(tokens[i].value))
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
        
        ASTNode* newNode = new ASTNode("SELECT");
        node->children.push_back(newNode);
        AstTree::BuildNode(newNode, tokens, i);

        return;


        if (tokens[i].value == "FROM") 
        {
            if(tokens[++i].value == "(")
            {
                ASTNode* subQueryNode = new ASTNode("SELECT");
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
            ASTNode* whereNode = new ASTNode("WHERE");
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
            ASTNode* groupByNode = new ASTNode("GROUP BY");

            while(i < tokens.size() && ( tokens[i].value != "HAVING" && tokens[i].value != ";" && tokens[i].value != "ORDER"))
                groupByNode->groupBy.columns.push_back(tokens[i++].value);

            if(tokens[i].value == "HAVING")
            {
                groupByNode->groupBy.having = new ASTNode("HAVING");
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
            ASTNode* orderByNode = new ASTNode("ORDER BY");
            orderByNode->orderBy.column = tokens[i++].value;
            
            orderByNode->orderBy.direction = tokens[i++].value; // ASC or DESC
            node->children.push_back(orderByNode);
        }

        startingDepth = i;
    }

    void AstTree::BuildSelectNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth)
    {
        node->type = "SELECT";
        int i = startingDepth;

        i++;
        while (tokens[i].value != "FROM") 
        {
            if (tokens[i].value != ",") 
                node->columns.push_back(tokens[i].value);

            //add check for instead of columns to use
            if(tokens[i].value.contains("@"))
                break;
            i++;
        }

        if (tokens[i].value == "FROM") 
        {
            if(tokens[++i].value == "(")
            {
                ASTNode* subQueryNode = new ASTNode("SELECT");
                node->children.push_back(subQueryNode);

                AstTree::BuildNode(subQueryNode, tokens, ++i);
            }
            else
                node->table = tokens[i].value;

            //skip closing bracket or table
            i++;
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