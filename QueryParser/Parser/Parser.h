#include <cstdint>
#include <iostream>
#include <ostream>
#include <string>
#include <variant>
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

    static Dictionary<WordType, string> wordTypeLiterals = {
        {WordType::Number, "Number"},
        {WordType::String, "String"}
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

    struct Node {
        // vector<ColumnOperation> columns;
        // Token table;
        // Token tableAlias;
        // struct{
        //     vector<WhereOperation> operations;
        //     vector<Token> operationCondition;
        // }whereClause;
        // struct {
        //     string column;
        //     string direction;
        // } orderBy;
        // struct {
        //     vector<string> columns;
        //     Node* having;
        // } groupBy;
        WordType type;  // "SELECT", "FROM", "WHERE", etc.
        variant<string, int64_t> value;
        vector<Node*> children; // Nested queries or joins
    
        Node();
        ~Node();  // Destructor

        ostream& operator<<(const Node& node);
    };

    class Parser{
        Node* root;

        ~Parser();
        Parser();

        static Node* ParseNumber(const Token& token);
        static Node* ParseString(const Token& token);

        public:
            static Parser& Get()
            {
                static Parser instance;

                return instance;
            }

            Node* Parse(vector<Token>& tokens);
    };


    // class AstTree
    // {
    //     Node* root;
    //     ~AstTree();
    //     AstTree();

    //     static bool IsNumber(const string& str);
    //     static void InitializeColumnOperations(vector<SelectColumn>& columns, vector<SelectColumn>& operations, Token& alias);
    //     static WhereOperation BuildJoinCondition(vector<Token>& tokens, int& depth);
    //     static KeyWord SetAppropriateJoinKeyword(vector<Token>& tokens, int& depth);
    //     void SearchQueryForJoinOperations(vector<Token>& tokens);

    //     public:
    //         static AstTree& Get()
    //         {
    //             static AstTree instance;
    //             return instance;
    //         }

    //         Node* BuildTree(vector<Token>& tokens);

    //         static void BuildNode(Node*& node, vector<Token>& tokens, int& startingDepth);
    //         static void BuildSelectNode(Node*& node, vector<Token>& tokens, int& startingDepth);
    //         static void BuildInsertNode(Node*& node, vector<Token>& tokens, int& startingDepth);
    //         static void BuildUpdateNode(Node*& node, vector<Token>& tokens, int& startingDepth);
    //         static void BuildDeleteNode(Node*& node, vector<Token>& tokens, int& startingDepth);
    //         static void BuildJoinNode(Node*& node, vector<Token>& tokens, int& startingDepth);
    //     };


}