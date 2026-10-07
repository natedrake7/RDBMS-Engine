#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringView.h>
#include <Systemic/DataTypes/Decimal.h>
#include <Systemic/DataTypes/StringValue.h>
#include <Systemic/DataTypes/DateTime.h>
#include <Systemic/DataTypes/Guid.h>
#include <Systemic/DataTypes/JsonBinary.h>
#include <Systemic/Reflection/EnumNames.h>

static constexpr auto TRUE_STRING = DataTypes::StringView("TRUE");
static constexpr auto FALSE_STRING = DataTypes::StringView("FALSE");
static constexpr auto NULL_STRING = DataTypes::StringView("NULL");

template<std::size_t N>
struct CaseInsensitiveStringSet{
    DataTypes::StringView values[N];

    static constexpr auto SIZE = N;

    [[nodiscard]] constexpr bool Contains(const DataTypes::StringView& text) const noexcept{
        return std::ranges::any_of(this->values, [&](const DataTypes::StringView& value){
             return DataTypes::StringView::EqualsIgnoreCase(value, text);
        });
    }

    [[nodiscard]] constexpr const DataTypes::StringView* begin() const noexcept{ return this->values; }
    [[nodiscard]] constexpr const DataTypes::StringView* end() const noexcept{ return this->values + N; }
};

inline static constexpr CaseInsensitiveStringSet TrueStrings{.values = { "true", "1", "yes", "y", "on" }};
inline static constexpr CaseInsensitiveStringSet FalseStrings{.values = { "false", "0", "no", "n", "off" }};

static_assert(
    TrueStrings.SIZE == FalseStrings.SIZE,
    "Number of true strings applied must be equal to false strings"
);

inline static constexpr auto BOOL_STRINGS_SIZE = TrueStrings.SIZE;

namespace DataTypes{
    // Any spelling above, in any case; nullopt when the text is not a boolean.
    constexpr std::optional<bool> ParseBool(const StringView& text){
        if (TrueStrings.Contains(text))
            return true;
        if (FalseStrings.Contains(text))
            return false;
        return std::nullopt;
    }
}

template<typename T>
inline constexpr block_size_t VECTOR_ENTRY_SIZE = sizeof(T);

template<>
inline constexpr block_size_t VECTOR_ENTRY_SIZE<void> = 0;

static constexpr auto VECTOR_COLUMN_SIZES_BY_DATATYPE =
    []<std::size_t... I>(std::index_sequence<I...>){
    return std::array<block_size_t, DATATYPE_COUNT>{
        VECTOR_ENTRY_SIZE<DataTypes::DataTypeStorage::At<I>>...
    };
}(std::make_index_sequence<DATATYPE_COUNT>{});

namespace DataTypes{
    inline static constexpr bool IsColumnType(const DataType type){
        return type != DataType::Null;
    }

    inline static constexpr bool IsVariableLengthType(const DataType type){
        return type == DataType::String || type == DataType::Json || type == DataType::Decimal;
    }

    inline static constexpr block_size_t FixedColumnSize(const DataType type){
        return IsVariableLengthType(type)
            ? 0
            : VECTOR_COLUMN_SIZES_BY_DATATYPE[static_cast<std::size_t>(type)];
    }

    inline static constexpr std::optional<DataType> ColumnTypeFromName(const StringView& name){
        const auto type = Reflection::EnumFromName<DataType>(name);
        return type && IsColumnType(*type)
            ? type
            : std::optional<DataType>();
    }
}

struct ColumnTypesByName{
    [[nodiscard]] inline static constexpr bool TryGetValue(const DataTypes::StringView& name, DataType& value)noexcept {
        const auto found = DataTypes::ColumnTypeFromName(name);
        if (found)
            value = *found;
        return found.has_value();
    }

    [[nodiscard]] inline static constexpr DataType Get(const DataTypes::StringView& name)noexcept{
        return DataTypes::ColumnTypeFromName(name).value_or(DataType::String);
    }

    [[nodiscard]] static constexpr DataType Get(const DataTypes::StringView* name) noexcept{
        return Get(*name);
    }
};

struct ColumnSizesByName{
    [[nodiscard]] inline static constexpr bool TryGetValue(const DataTypes::StringView& name, block_size_t& size)noexcept{
        const auto type = DataTypes::ColumnTypeFromName(name);
        if (type.has_value())
            size = DataTypes::FixedColumnSize(type.value());
        return type.has_value();
    }

    [[nodiscard]] inline static constexpr block_size_t Get(const DataTypes::StringView& name)noexcept{
        const auto type = DataTypes::ColumnTypeFromName(name);
        return DataTypes::FixedColumnSize(type.value_or(DataType::Null));
    }

    [[nodiscard]] inline static constexpr block_size_t Get(const DataTypes::StringView* name)noexcept{
        return Get(*name);
    }
};

static inline constexpr ColumnTypesByName COLUMN_TYPENAMES_TO_ENUMS{};
static inline constexpr ColumnSizesByName COLUMN_SIZES_BY_TYPENAME{};

static constexpr auto SQL_TYPES_NAMES = []<std::size_t... I>(std::index_sequence<I...>){
    return std::array{ Reflection::EnumName(static_cast<DataType>(I))... };
}(std::make_index_sequence<DATATYPE_COUNT>{});

static constexpr DataType JSON_TYPES_NAMES[]{
    DataType::Null, //NULL
    DataType::Bool,
    DataType::Decimal,
    DataType::String,
    DataType::String, //Array
    DataType::String, //Object
};

static_assert(
    Reflection::IsContiguous<Serialization::JsonType>()
    && std::size(JSON_TYPES_NAMES) == static_cast<std::size_t>(Reflection::EnumCount<Serialization::JsonType>),
    "JSON_TYPES_NAMES needs exactly one entry per JsonType"
);