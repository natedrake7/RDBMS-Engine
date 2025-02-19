#include "ASTTree.h"
#include "../Tokenizer/Tokenizer.h"

namespace QueryParser 
{
    ASTNode::ASTNode(string type) : type(type) {}

    ASTNode::~ASTNode() 
    {
        for (auto child : children) 
        {
            delete child;
        }
    }

    AstTree::AstTree() : root(nullptr) {}
    
    AstTree::~AstTree()
    {
        delete root;
    };

    ASTNode* AstTree::BuildTree(vector<Token>& tokens) 
    {
        root = new ASTNode("SELECT");
        int i = 0;
    
        if (tokens[i].value == "SELECT") 
        {
            i++;
            while (tokens[i].value != "FROM") {
                if (tokens[i].value != ",") root->columns.push_back(tokens[i].value);
                i++;
            }
        }
    
        if (tokens[i].value == "FROM") 
        {
            root->table = tokens[++i].value;
            i++;
        }
    
        if (tokens[i].value == "WHERE") 
        {
            ASTNode* whereNode = new ASTNode("WHERE");
            whereNode->whereClause.column = tokens[++i].value;
            whereNode->whereClause.op = tokens[++i].value;
            whereNode->whereClause.value = tokens[++i].value;
            root->children.push_back(whereNode);
            i++;
        }
    
        if (i < tokens.size() && tokens[i].value == "ORDER") 
        {
            i += 2;  // Skip "ORDER BY"
            ASTNode* orderByNode = new ASTNode("ORDER BY");
            orderByNode->orderBy.column = tokens[i++].value;
            orderByNode->orderBy.direction = tokens[i].value; // ASC or DESC
            root->children.push_back(orderByNode);
        }

        return root;
    }
}