#include <string>
#include <vector>
#include "../Tokenizer/Tokenizer.h"

using namespace std;

namespace QueryParser{

    struct Token;

    typedef struct{
        vector<Token> columns;
        vector<Token> operation;

        Token alias;
    }ColumnOperation;

    struct ASTNode {
        KeyWord type;  // "SELECT", "FROM", "WHERE", etc.
        vector<ColumnOperation> columns;
        Token table;
        struct {
            string column;
            string op;
            string value;
        } whereClause;
        struct {
            string column;
            string direction;
        } orderBy;
        struct {
            vector<string> columns;
            ASTNode* having;
        } groupBy;
        vector<ASTNode*> children; // Nested queries or joins
    
        ASTNode();
        ~ASTNode();  // Destructor
    };

    class AstTree
    {
        ASTNode* root;
        ~AstTree();
        AstTree();

        static bool IsNumber(const string& str);
        static void InitializeColumnOperations(vector<Token>& columns, vector<Token>& operations, Token& alias);
        
        public:
            static AstTree& Get()
            {
                static AstTree instance;
                return instance;
            }

            ASTNode* BuildTree(vector<Token>& tokens);

            static void BuildNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
            static void BuildSelectNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
            static void BuildInsertNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
            static void BuildUpdateNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
            static void BuildDeleteNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
    };


}