#pragma once
#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace Network::ResultFormat{
    inline constexpr  UnsignedTinyInt COLUMN_IS_CONSTANT = 1 << 0;


    /**
     * This function checks if an engine datatype is variable length for the Network.
     * In this sense Decimals are considered variable length as they are converted to string when returned to a client
     * @param type datatype to check
     * @return returns true if the provided type is a variable length column for the client connection
     */
    [[nodiscard]] inline constexpr bool IsVariableLengthColumn(const DataType type){
        return
                type == DataType::String
                || type == DataType::Json
                || type == DataType::Decimal;
    }

    [[nodiscard]] inline constexpr UnsignedInt ValidityBytes(const Int rowCount){
        return (rowCount + 7) / 8;
    }


}
