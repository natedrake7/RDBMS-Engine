#pragma once
#include <array>
#include <concepts>
#include <meta>
#include <optional>
#include <string>
#include <string_view>

#include <Systemic/DataStructures/PolymorphicArray.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/Value.h>
#include <Systemic/Memory/IAllocator.h>
#include <Systemic/Reflection/Enum.h>

// Not CoreEngine::Reflection: inside namespace CoreEngine that name would hide Systemic's ::Reflection
// (Enumerators, EnumCount, HasAnnotation) for every file that sees this header.
namespace CoreEngine::Rows{
    /**
  * @name Annotations
  * Annotation values must be structural: strings are stored inline, never as pointers to literals.
  * @{
  */

    struct FixedString{
        static constexpr data_size_t CAPACITY = 128;
        char _data[CAPACITY]{};         // fully initialized: annotation values must be constants
        data_size_t _size = 0;

        // implicit on purpose: annotations are written as Default{"1"}, TableInfo{"sys_tables", ...}
        consteval FixedString(const char* text){
            while (text[this->_size] != '\0'){
                if (this->_size == CAPACITY)
                    throw "FixedString: text longer than CAPACITY";
                this->_data[this->_size] = text[this->_size];
                ++this->_size;
            }
        }

        [[nodiscard]] constexpr std::string_view View()const{
            return {this->_data, this->_size};
        }
    };

    struct TableInfo{
        FixedString name;
        Int id;
        UnsignedTinyInt ordinal;
    };

    struct PrimaryKey{};

    struct Identity{
        BigInt _start = 1;
    };

    struct MaxSize{
        block_size_t _value;
    };

    struct Default{
        FixedString _sql;
    };

    struct ColumnName{
        FixedString _name;
    };

    template <typename TAnnotation>
    [[nodiscard]] consteval TAnnotation AnnotationOf(const std::meta::info item){
        return std::meta::extract<TAnnotation>(std::meta::annotations_of_with_type(item, ^^TAnnotation)[0]);
    }

    /** @} End of: Annotations */

    template<typename TRow>
    consteval auto Members(){
        return std::define_static_array(std::meta::nonstatic_data_members_of(^^TRow, std::meta::access_context::unchecked()));
    }

    template <typename TRow>
    inline constexpr column_number_t ColumnCount = Members<TRow>().size();

    template <typename T> inline constexpr bool IsNullable = false;
    template <typename T> inline constexpr bool IsNullable<std::optional<T>> = true;

    template <typename T> struct Unwrap{ using Type = T; };
    template <typename T> struct Unwrap<std::optional<T>>{ using Type = T; };

    [[nodiscard]] consteval std::string_view SnakeCase(const std::string_view identifier){
        std::string out;
        for (std::size_t i = 0; i < identifier.size(); i++){
            const auto c = identifier[i];
            if (c >= 'A' && c <= 'Z'){
                if (i > 0) out.push_back('_');
                out.push_back(static_cast<char>(c - 'A' + 'a'));
            }
            else
                out.push_back(c);
        }
        return std::define_static_string(out);
    }

    [[nodiscard]] consteval std::string_view ColumnNameOf(const std::meta::info member){
        // ::Reflection is Systemic's namespace (HasAnnotation)
        return ::Reflection::HasAnnotation<ColumnName>(member)
            ? std::define_static_string(AnnotationOf<ColumnName>(member)._name.View())
            : SnakeCase(std::meta::identifier_of(member));
    }

    [[nodiscard]] consteval column_index_t OrdinalOf(const std::meta::info member){
        column_index_t ordinal = 0;
        for (const auto candidate : std::meta::nonstatic_data_members_of(std::meta::parent_of(member), std::meta::access_context::unchecked())){
            if (candidate == member)
                return ordinal;
            ++ordinal;
        }
        throw "Rows::OrdinalOf: not a member of its parent";
    }

    template <typename TRow>
    [[nodiscard]] consteval auto KeyColumnsOf(){
        constexpr auto count = []{
            std::size_t keys = 0;
            for (const auto member : Members<TRow>())
                keys += ::Reflection::HasAnnotation<PrimaryKey>(member);
            return keys;
        }();

        std::array<column_index_t, count> keys{};
        std::size_t next = 0;
        column_index_t ordinal = 0;
        for (const auto member : Members<TRow>()){
            if (::Reflection::HasAnnotation<PrimaryKey>(member))
                keys[next++] = ordinal;
            ++ordinal;
        }
        return keys;
    }


    template <typename T>
    Value MakeValue(const T& field, const ::Memory::IAllocator* allocator, const column_index_t ordinal){
        if constexpr (std::same_as<T, DataTypes::String>)
            return Value(DataTypes::StringView::ViewOf(field), allocator, ordinal);
        else
            return Value(field, ordinal);
    }

    // Nullable column: empty optional -> NULL (more specialized, so it wins over the overload above)
    template <typename T>
    Value MakeValue(const std::optional<T>& field, const ::Memory::IAllocator* allocator, const column_index_t ordinal){
        return field.has_value() ? MakeValue(*field, allocator, ordinal) : Value::Null(ordinal);
    }

    template <typename TRow>
    DataStructures::PolymorphicArray<Value> ToValues(const TRow& row, const ::Memory::IAllocator* allocator){
        DataStructures::PolymorphicArray<Value> values(allocator, ColumnCount<TRow>);
        column_index_t ordinal = 0;
        template for (constexpr auto member : Members<TRow>()){
            using T = [:std::meta::type_of(member):];
            const auto& field = row.[:member:];
            if constexpr (IsNullable<T>)
                values.Push(field.has_value() ? MakeValue(*field, allocator, ordinal) : Value::Null(ordinal));
            else if constexpr (::Reflection::HasAnnotation<Identity>(member)){
                if (field != T{})
                    values.Push(MakeValue(field, allocator, ordinal));
            }
            else
                values.Push(MakeValue(field, allocator, ordinal));

            ordinal++;
        }
        return values;
    }

    template <typename TRow>
    TRow FromRow(const DataStructures::PolymorphicArray<Value>& data, const ::Memory::IAllocator* allocator){
        TRow row{};
        column_index_t ordinal = 0;
        template for (constexpr auto member : Members<TRow>()){
            using T = [:std::meta::type_of(member):];
            const auto& value = data[ordinal++];
            if constexpr (IsNullable<T>){
                if (!value.IsNull())
                    row.[:member:] = value.template Get<typename T::value_type>(allocator);
            }
            else
                row.[:member:] = value.template Get<T>(allocator);
        }
        return row;
    }
}
