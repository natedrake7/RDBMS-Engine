#include "../../include/Decoding/ResultDecoder.h"
#include <ostream>

#include "../../../Server/include/ClientConnection.h"
#include "Network/PayloadReader.h"

namespace Client{
    bool ResultDecoder::DecodeRowDescription(const std::vector<char>* payload){
        Network::PayloadReader reader(payload->data(), payload->size());

        Int columnCount = 0;
        if (!reader.Read<Int>(columnCount))
            return false;

        this->_columns.resize(columnCount);
        for (auto& column : this->_columns){
            if (!reader.ReadString(column._name) || !reader.Read<DataType>(column._type))
                return false;
        }

        return true;
    }

    void ResultDecoder::PrintHeader(std::ostream& os) const{
        for (const auto& [_name, _type] : this->_columns)
            os << _name << " || ";

        os << '\n';
    }
}
