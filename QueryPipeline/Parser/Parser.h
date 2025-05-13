#pragma once

#include "../../Database/B+Tree/BPlusTree.h"


#include <iostream>


using namespace std;

namespace QueryParser{

    class Parser{
        ~Parser();
        Parser();

        public:
            static Parser& Get()
            {
                static Parser instance;

                return instance;
            }

            static void Parse(const DatabaseEngine::Database& db);
    };

}