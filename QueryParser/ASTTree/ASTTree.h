#include <string>
#include <vector>
#include "../../AdditionalLibraries/Dictionary/Dictionary.h"

using namespace std;

namespace QueryParser{

    enum class KeyWord: uint8_t{
        Select = 1,
        Insert = 2,
        Update = 3,
        Delete = 4,
        From = 5,
        Where = 6,
        Group = 7,
        By = 8,
        Having = 9,
        Order = 10,
        Asc = 11,
        Desc = 12,
        Into = 13
    };

    static Dictionary<string, KeyWord> keywordsDictionary = {
        {"SELECT", KeyWord::Select},
        {"INSERT", KeyWord::Insert},
        {"UPDATE", KeyWord::Update},
        {"DELETE", KeyWord::Delete},
        {"FROM", KeyWord::From},
        {"WHERE", KeyWord::Where},
        {"GROUP", KeyWord::Group},
        {"BY", KeyWord::By},
        {"HAVING", KeyWord::Having},
        {"ORDER", KeyWord::Order},
        {"ASC", KeyWord::Asc},
        {"DESC", KeyWord::Desc},
        {"INTO", KeyWord::Into}
    };

    struct Token;

    struct ASTNode {
        string type;  // "SELECT", "FROM", "WHERE", etc.
        vector<string> columns;
        string table;
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
    
        ASTNode(string type);
        ~ASTNode();  // Destructor
    };

    class AstTree
    {
        ASTNode* root;
        ~AstTree();
        AstTree();
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