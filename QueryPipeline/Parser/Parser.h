#pragma once

#include "../../Database/B+Tree/BPlusTree.h"
#include "../Statements/Statements.h"


#include <any>
#include <iostream>


using namespace std;

namespace QueryPipeline{

    class Parser{
        ~Parser();
        Parser();

        static Statements::Statement* CreateStatement(const std::any &ast);

        public:
            static Parser& Get()
            {
                static Parser instance;

                return instance;
            }

            static void Parse(const string& query);
    };

}