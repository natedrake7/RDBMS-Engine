#include <string>
#include <vector>

using namespace std;

namespace QueryParser{
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

    // HashSet<string> KeywordsDictionary = 
    // {
    //     "SELECT", 
    //     "FROM", 
    //     "WHERE", 
    //     "GROUP BY", 
    //     "HAVING", 
    //     "ORDER BY", 
    //     "ASC", 
    //     "DESC"
    // };

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

            static void BuildTree(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
            static void BuildSelectNode(ASTNode*& node, vector<Token>& tokens, int& startingDepth);
    };


}