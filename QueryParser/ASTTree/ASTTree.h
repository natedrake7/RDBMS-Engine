#include <string>
#include <vector>
#include "../Tokenizer/Tokenizer.h"

using namespace std;

namespace QueryParser{

    static Dictionary<string, KeyWord> joinTypeKeywordDictionary = {
        {"INNER", KeyWord::InnerJoin},
        {"LEFT",  KeyWord::LeftJoin},
        {"RIGHT", KeyWord::RightJoin},
        {"FULL",  KeyWord::FullJoin},
        {"JOIN", KeyWord::InnerJoin}
    };

    struct Token;

    typedef struct SelectColumn : Token{
        Token subToken;
    } SelectColumn;

    typedef struct {
        Token leftOperand;
        Token operation;
        Token rightOperand;
    }WhereOperation;

    typedef struct{
        vector<SelectColumn> columns;
        vector<SelectColumn> operation;

        Token alias;
    }ColumnOperation;

    struct ASTNode {
        KeyWord type;  // "SELECT", "FROM", "WHERE", etc.
        vector<ColumnOperation> columns;
        Token table;
        Token tableAlias;
        struct{
            vector<WhereOperation> operations;
            vector<Token> operationCondition;
        }whereClause;
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
        vector<int> LeftJoinIndexes;
        vector<int> RightJoinIndexes;
        vector<int> FullJoinIndexes;
        vector<int> InnerJoinIndexes;
        ~AstTree();
        AstTree();

        static bool IsNumber(const string& str);
        static void InitializeColumnOperations(vector<SelectColumn>& columns, vector<SelectColumn>& operations, Token& alias);
        static WhereOperation BuildJoinCondition(vector<Token>& tokens, int& depth);
        static KeyWord SetAppropriateJoinKeyword(vector<Token>& tokens, int& depth);
        void SearchQueryForJoinOperations(vector<Token>& tokens);

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
            static void BuildJoinNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
        };


}