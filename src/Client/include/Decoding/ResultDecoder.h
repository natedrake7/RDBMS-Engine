#pragma once
#include <string>
#include <vector>

enum class DataType : uint8_t;

namespace Client{
    class ResultDecoder{
        struct ColumnDescription{
            std::string _name;
            DataType _type;
        };

        std::vector<ColumnDescription> _columns;

        public:
            [[nodiscard]] bool DecodeRowDescription(const std::vector<char>* payload);

            void PrintHeader(std::ostream& os)const;


    };
}
