#pragma once

#include <Systemic/Comparators.h>
#include <Systemic/DataTypes/String.h>
#include <Systemic/DataStructures/StaticArray.h>
#include <Systemic/Functions/MathFunctions.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Constants.h>


namespace DataTypes {
    class StringView;

    enum class DecimalRoundingMode : UnsignedTinyInt{
        Truncate = 0,
        HalfUp = 1,
        HalfEven = 2
    };

    enum class DecimalStatus : UnsignedTinyInt{
        Ok = 0,
        Overflow = 1,
        DivideByZero = 2,
        InvalidFormat = 3
    };

    inline constexpr Int DECIMAL_MINIMUM_DIVISION_SCALE = 6;
    inline constexpr Int DECIMAL_STRING_SIZE = MAX_DECIMAL_PRECISION + 3;   // sign + leading zero + '.' + digits
    inline constexpr Int DECIMAL_KEY_SIZE = 16;

    struct DecimalType{
        UnsignedTinyInt _precision;
        UnsignedTinyInt _scale;

        constexpr explicit DecimalType()
            : _precision(0), _scale(0){}
        constexpr explicit DecimalType(const Int precision, const Int scale)
            : _precision(static_cast<UnsignedTinyInt>(precision)), _scale(static_cast<UnsignedTinyInt>(scale)){}

        [[nodiscard]] friend constexpr bool operator==(const DecimalType&, const DecimalType&) = default;

        [[nodiscard]] constexpr bool IsValid() const{
            return this->_precision >= 1
            && this->_precision <= MAX_DECIMAL_PRECISION
            && this->_scale <= this->_precision;
        }

        [[nodiscard]] static constexpr Int IntegerDigits(const Int precision, const Int scale){
            return precision - scale;
        }

        [[nodiscard]] constexpr Int IntegerDigits()const{
            return this->_precision - this->_scale;
        }

        [[nodiscard]] static constexpr DecimalType Capped(const Int precision, const Int scale){
            if (precision < MAX_DECIMAL_PRECISION)
                return DecimalType(precision, scale);

            const auto integerDigits = IntegerDigits(precision, scale);
            const auto keptScale = Math::Max(
                Math::Min(scale, DECIMAL_MINIMUM_DIVISION_SCALE),
                MAX_DECIMAL_PRECISION - integerDigits
            );
            return DecimalType(MAX_DECIMAL_PRECISION, Math::Min(scale, keptScale));
        }

        [[nodiscard]] static constexpr DecimalType ForAdd(const DecimalType lhs, const DecimalType rhs){
            const auto scale = Math::Max(lhs._scale, rhs._scale);
            return Capped(Math::Max(lhs.IntegerDigits(), rhs.IntegerDigits()) + scale + 1, scale);
        }

        // The largest product of p1 and p2 digits always fits in p1 + p2 digits
        [[nodiscard]] static constexpr DecimalType ForMultiply(const DecimalType lhs, const DecimalType rhs){
            return Capped(lhs._precision + rhs._precision, lhs._scale + rhs._scale);
        }

        [[nodiscard]] static constexpr DecimalType ForDivide(const DecimalType lhs, const DecimalType rhs){
            const Int scale = Math::Max(DECIMAL_MINIMUM_DIVISION_SCALE, lhs._scale + rhs._precision + 1);
            return Capped(lhs.IntegerDigits() + rhs._scale + scale, scale);
        }

        template<IsInteger T>
        [[nodiscard]] static consteval DecimalType ForInteger(){
            return DecimalType(Math::Min(Math::LimitsDigits10<T>() + 1, MAX_DECIMAL_PRECISION), 0);
        }

        [[nodiscard]] static constexpr bool CanAddOverflow(const DecimalType lhs, const DecimalType rhs){
            return Math::Max(lhs.IntegerDigits(), rhs.IntegerDigits()) + Math::Max(lhs._scale, rhs._scale) + 1 > MAX_DECIMAL_PRECISION;
        }

        [[nodiscard]] static constexpr bool CanMultiplyOverflow(const DecimalType lhs, const DecimalType rhs){
            return lhs._precision + rhs._precision > MAX_DECIMAL_PRECISION;
        }

        // Dropping digits can round up by one integer digit: 999.995 needs 4 of them at scale 2
        [[nodiscard]] static constexpr bool CanCastOverflow(const DecimalType from, const DecimalType to){
            return to._scale >= from._scale
                ? from.IntegerDigits() > to.IntegerDigits()
                : from.IntegerDigits() >= to.IntegerDigits();
        }
    };

    namespace DecimalDetail
    {
        inline constexpr auto POW10 = []{
            std::array<UnsignedHugeInt, MAX_DECIMAL_PRECISION + 1> table{};
            table[0] = 1;
            for (Int i = 1; i <= MAX_DECIMAL_PRECISION; i++)
                table[i] = table[i - 1] * 10;
            return table;
        }();

        inline constexpr Int WIDE_STEP_DIGITS = 19;

        inline constexpr UnsignedHugeInt MAX_MAGNITUDE = POW10[MAX_DECIMAL_PRECISION] - 1;

        struct Wide{
            UnsignedBigInt _limbs[4]{};

            [[nodiscard]] static constexpr Wide From(const UnsignedHugeInt value){
                Wide wide;
                wide._limbs[0] = static_cast<UnsignedBigInt>(value);
                wide._limbs[1] = static_cast<UnsignedBigInt>(value >> 64);
                return wide;
            }

            [[nodiscard]] constexpr bool FitsInHugeInt() const{
                return (this->_limbs[2] | this->_limbs[3]) == 0;
            }

            [[nodiscard]] constexpr bool IsOdd() const{
                return (this->_limbs[0] & 1) != 0;
            }

            [[nodiscard]] constexpr UnsignedHugeInt LowHugeInt() const{
                return (static_cast<UnsignedHugeInt>(this->_limbs[1]) << 64) | this->_limbs[0];
            }

            [[nodiscard]] static constexpr Wide Multiply(const UnsignedHugeInt left, const UnsignedHugeInt right){
                const auto leftLimbs[2] = {
                    static_cast<UnsignedBigInt>(left),
                    static_cast<UnsignedBigInt>(left >> 64)
                };

                const auto rightLimbs[2] = {
                    static_cast<UnsignedBigInt>(right),
                    static_cast<UnsignedBigInt>(right >> 64)
                };

                Wide result;
                for (Int i = 0; i < 2; i++){
                    UnsignedHugeInt carry = 0;
                    for (Int j = 0; j < 2; j++){
                        carry += static_cast<UnsignedHugeInt>(leftLimbs[i]) * rightLimbs[j] + result._limbs[i + j];
                        result._limbs[i + j] = static_cast<UnsignedBigInt>(carry);
                        carry >>= 64;
                    }
                    result._limbs[i + 2] = static_cast<UnsignedBigInt>(carry);
                }
                return result;
            }

            [[nodiscard]] constexpr bool MultiplySmall(const UnsignedBigInt factor){
                UnsignedHugeInt carry = 0;
                for (auto& limb : this->_limbs){
                    carry += static_cast<UnsignedHugeInt>(limb) * factor;
                    limb = static_cast<UnsignedBigInt>(carry);
                    carry >>= 64;
                }
                return carry == 0;
            }

            [[nodiscard]] constexpr bool MultiplyPow10(Int digits){
                while (digits > 0){
                    const auto step = Math::Min(digits, WIDE_STEP_DIGITS);
                    if (!this->MultiplySmall(static_cast<UnsignedBigInt>(POW10[step])))
                        return false;
                    digits -= step;
                }
                return true;
            }

            constexpr UnsignedBigInt DivideSmall(const UnsignedBigInt divisor){
                UnsignedHugeInt remainder = 0;
                for (Int i = 3; i >= 0; i--){
                    const auto current = (remainder << 64) | this->_limbs[i];
                    this->_limbs[i] = static_cast<UnsignedBigInt>(current / divisor);
                    remainder = current % divisor;
                }
                return static_cast<UnsignedBigInt>(remainder);
            }

            [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const Wide& lhs, const Wide& rhs){
                for (Int i = 3; i >= 0; i--){
                    if (lhs._limbs[i] != rhs._limbs[i])
                        return lhs._limbs[i] <=> rhs._limbs[i];
                }
                return std::strong_ordering::equal;
            }

            constexpr void ShiftLeftOne(){
                for (Int i = 3; i > 0; i--)
                    this->_limbs[i] = (this->_limbs[i] << 1) | (this->_limbs[i - 1] >> 63);
                this->_limbs[0] <<= 1;
            }

            constexpr void Subtract(const Wide& other){
                UnsignedBigInt borrow = 0;
                for (Int i = 0; i < 4; i++){
                    const auto value = this->_limbs[i];
                    const auto subtrahend = other._limbs[i];
                    const auto difference = value - subtrahend - borrow;
                    borrow = (value < subtrahend) || (value == subtrahend && borrow != 0) ? 1 : 0;
                    this->_limbs[i] = difference;
                }
            }

            [[nodiscard]] static constexpr Wide Divide(const Wide& dividend, const Wide& divisor, Wide& remainder){
                Wide quotient;
                remainder = Wide();
                for (Int bit = 255; bit >= 0; bit--){
                    remainder.ShiftLeftOne();
                    remainder._limbs[0] |= (dividend._limbs[bit / 64] >> (bit % 64)) & 1;
                    if (remainder >= divisor){
                        remainder.Subtract(divisor);
                        quotient._limbs[bit / 64] |= UnsignedBigInt{1} << (bit % 64);
                    }
                }
                return quotient;
            }
        };

        [[nodiscard]] constexpr bool ShouldRoundUp(
            const DecimalRoundingMode mode,
            const Int firstDroppedDigit,
            const bool restIsNonZero,
            const bool isOdd
        ){
            switch (mode){
            case DecimalRoundingMode::Truncate:
                return false;
            case DecimalRoundingMode::HalfUp:
                return firstDroppedDigit >= 5;
            case DecimalRoundingMode::HalfEven:
                return firstDroppedDigit > 5 || (firstDroppedDigit == 5 && (restIsNonZero || isOdd));
            }
            return false;
        }

        // value / 10^digits, rounded by magnitude. Used only when the value may not fit in 128 bits.
        [[nodiscard]] constexpr Wide DividePow10Rounded(Wide value, Int digits, const DecimalRoundingMode mode, bool& carriedOut){
            carriedOut = false;
            if (digits <= 0)
                return value;

            bool restIsNonZero = false;
            while (digits > 1){
                const auto step = std::min(digits - 1, WIDE_STEP_DIGITS);
                restIsNonZero |= value.DivideSmall(static_cast<UnsignedBigInt>(POW10[step])) != 0;
                digits -= step;
            }

            const auto dropped = static_cast<Int>(value.DivideSmall(10));
            if (ShouldRoundUp(mode, dropped, restIsNonZero, value.IsOdd())){
                carriedOut = true;
                for (auto& limb : value._limbs){
                    if (++limb != 0){
                        carriedOut = false;
                        break;
                    }
                }
            }

            return value;
        }

        [[nodiscard]] constexpr bool IsDigit(const char c){ return c >= '0' && c <= '9'; }
    }

    class [[gnu::packed]] Decimal final {
        HugeInt _mantissa;

        using StringBuffer = DataStructures::StaticArray<char, DECIMAL_STRING_SIZE>;

        [[nodiscard]] constexpr UnsignedHugeInt Magnitude() const{
            return this->_mantissa < 0
                ? UnsignedHugeInt{0} - static_cast<UnsignedHugeInt>(this->_mantissa)
                : static_cast<UnsignedHugeInt>(this->_mantissa);
        }

        [[nodiscard]] static constexpr Decimal FromMagnitude(const UnsignedHugeInt magnitude, const bool isNegative){
            const auto mantissa = static_cast<HugeInt>(magnitude);
            return Decimal::FromMantissa(isNegative ? -mantissa : mantissa);
        }

        [[noreturn, gnu::cold]] static constexpr void Throw(const DecimalStatus status){
            switch (status){
            case DecimalStatus::Overflow:
                throw std::overflow_error("Decimal: value does not fit the precision");
            case DecimalStatus::DivideByZero:
                throw std::domain_error("Decimal: division by zero");
            default:
                throw std::invalid_argument("Decimal: invalid format");
            }
        }

        static constexpr void Check(const DecimalStatus status){
            if (status != DecimalStatus::Ok) [[unlikely]]
                Decimal::Throw(status);
        }

        constexpr explicit Decimal(const HugeInt mantissa)
            : _mantissa(mantissa){}

    public:
        constexpr explicit Decimal()
            : _mantissa(0){};

        [[nodiscard]] static constexpr Decimal FromMantissa(const HugeInt mantissa){
            return Decimal(mantissa);
        }

        [[nodiscard]] constexpr HugeInt Mantissa() const{ return this->_mantissa; }
        [[nodiscard]] constexpr bool IsNegative() const{ return this->_mantissa < 0; }
        [[nodiscard]] constexpr bool IsZero() const{ return this->_mantissa == 0; }

        [[nodiscard]] constexpr bool FitsPrecision(const Int precision) const{
            return this->Magnitude() < DecimalDetail::POW10[precision];
        }

        [[nodiscard]] static constexpr DecimalStatus TryAdd(const Decimal& left, const Decimal& right, Decimal* out){
            HugeInt sum = 0;
            if (__builtin_add_overflow(left.Mantissa(), right.Mantissa(), &sum))
                return DecimalStatus::Overflow;
            *out = Decimal::FromMantissa(sum);
            return out->Magnitude() > DecimalDetail::MAX_MAGNITUDE ? DecimalStatus::Overflow : DecimalStatus::Ok;
        }

        [[nodiscard]] static constexpr DecimalStatus TrySubtract(const Decimal& left, const Decimal& right, Decimal* out){
            HugeInt difference = 0;
            if (__builtin_sub_overflow(left.Mantissa(), right.Mantissa(), &difference))
                return DecimalStatus::Overflow;
            *out = Decimal::FromMantissa(difference);
            return out->Magnitude() > DecimalDetail::MAX_MAGNITUDE ? DecimalStatus::Overflow : DecimalStatus::Ok;
        }

        [[nodiscard]] static constexpr Decimal AddUnchecked(const Decimal& left, const Decimal& right){
            return Decimal::FromMantissa(left.Mantissa() + right.Mantissa());
        }

        [[nodiscard]] static constexpr Decimal SubtractUnchecked(const Decimal& left, const Decimal& right){
            return Decimal::FromMantissa(left.Mantissa() - right.Mantissa());
        }

        // Only when DecimalType::CanMultiplyOverflow is false
        [[nodiscard]] static constexpr Decimal MultiplyUnchecked(const Decimal& left, const Decimal& right){
            return Decimal::FromMantissa(left.Mantissa() * right.Mantissa());
        }
        [[nodiscard]] friend constexpr Decimal operator+(const Decimal& left, const Decimal& right){
            Decimal result;
            Decimal::Check(Decimal::TryAdd(left, right, &result));
            return result;
        }

        [[nodiscard]] friend constexpr Decimal operator-(const Decimal& left, const Decimal& right){
            Decimal result;
            Decimal::Check(Decimal::TrySubtract(left, right, &result));
            return result;
        }

        [[nodiscard]] friend constexpr Decimal operator-(const Decimal& value){
            return Decimal::FromMantissa(-value.Mantissa());
        }

        // The sign follows the dividend, as in C++ and SQL
        [[nodiscard]] friend constexpr Decimal operator%(const Decimal& left, const Decimal& right){
            if (right.IsZero())
                Decimal::Throw(DecimalStatus::DivideByZero);
            return Decimal::FromMantissa(left.Mantissa() % right.Mantissa());
        }

        [[nodiscard]] friend constexpr bool operator==(const Decimal& left, const Decimal& right){
            return left.Mantissa() == right.Mantissa();
        }

        [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const Decimal& left, const Decimal& right){
            return left.Mantissa() <=> right.Mantissa();
        }

        [[nodiscard]] static constexpr DecimalStatus TryMultiply(
            const Decimal& left,
            const Decimal& right,
            const Int droppedDigits,
            const DecimalRoundingMode mode,
            Decimal* out
        ){
            assert(droppedDigits >= 0 && "Decimal::TryMultiply: the result scale cannot exceed the sum of the input scales");

            // The common case: the exact product fits 128 bits
            HugeInt product = 0;
            if (droppedDigits <= MAX_DECIMAL_PRECISION && !__builtin_mul_overflow(left.Mantissa(), right.Mantissa(), &product)){
                if (droppedDigits == 0)
                    *out = Decimal::FromMantissa(product);
                else
                    (void)Decimal::TryRescale(Decimal::FromMantissa(product), droppedDigits, 0, mode, out);

                return out->Magnitude() > DecimalDetail::MAX_MAGNITUDE ? DecimalStatus::Overflow : DecimalStatus::Ok;
            }

            bool carriedOut = false;
            const auto rounded = DecimalDetail::DividePow10Rounded(
                DecimalDetail::Wide::Multiply(left.Magnitude(), right.Magnitude()),
                droppedDigits,
                mode,
                carriedOut
            );

            if (carriedOut || !rounded.FitsInHugeInt() || rounded.LowHugeInt() > DecimalDetail::MAX_MAGNITUDE)
                return DecimalStatus::Overflow;

            *out = Decimal::FromMagnitude(rounded.LowHugeInt(), left.IsNegative() != right.IsNegative());
            return DecimalStatus::Ok;
        }

            [[nodiscard]] static constexpr DecimalStatus TryDivide(
            const Decimal& left,
            const Decimal& right,
            const Int scaleShift,
            const DecimalRoundingMode mode,
            Decimal* out
        ){
            if (right.IsZero())
                return DecimalStatus::DivideByZero;

            const auto isNegative = left.IsNegative() != right.IsNegative();

            // A divisor this large rounds every 38-digit dividend to zero, and would not fit the shift below
            if (scaleShift <= -(MAX_DECIMAL_PRECISION + 1)){
                *out = Decimal();
                return DecimalStatus::Ok;
            }

            auto dividend = DecimalDetail::Wide::From(left.Magnitude());
            auto divisor = DecimalDetail::Wide::From(right.Magnitude());

            if (scaleShift >= 0){
                if (!dividend.MultiplyPow10(scaleShift))
                    return DecimalStatus::Overflow;
            }
            else if (!divisor.MultiplyPow10(-scaleShift))
                return DecimalStatus::Overflow;

            UnsignedHugeInt quotient = 0;
            bool roundUp = false;

            if (dividend.FitsInHugeInt() && divisor.FitsInHugeInt()){
                const auto numerator = dividend.LowHugeInt();
                const auto denominator = divisor.LowHugeInt();
                quotient = numerator / denominator;
                const auto remainder = numerator % denominator;
                const auto half = remainder > denominator - remainder ? 1 : remainder == denominator - remainder ? 0 : -1;
                roundUp = mode == DecimalRoundingMode::HalfUp
                    ? half >= 0
                    : mode == DecimalRoundingMode::HalfEven && (half > 0 || (half == 0 && (quotient & 1) != 0));
            }
            else{
                DecimalDetail::Wide remainder;
                const auto wideQuotient = DecimalDetail::Wide::Divide(dividend, divisor, remainder);
                if (!wideQuotient.FitsInHugeInt())
                    return DecimalStatus::Overflow;

                quotient = wideQuotient.LowHugeInt();
                remainder.ShiftLeftOne();
                const auto order = remainder <=> divisor;
                constexpr std::__cmp_cat::__literal_zero zeroOrder(nullptr);
                roundUp = mode == DecimalRoundingMode::HalfUp
                    ? order >= zeroOrder
                    : mode == DecimalRoundingMode::HalfEven && (order > zeroOrder || (order == zeroOrder && (quotient & 1) != 0));
            }

            if (quotient > DecimalDetail::MAX_MAGNITUDE)
                return DecimalStatus::Overflow;
            if (roundUp)
                quotient++;
            if (quotient > DecimalDetail::MAX_MAGNITUDE)
                return DecimalStatus::Overflow;

            *out = Decimal::FromMagnitude(quotient, isNegative);
            return DecimalStatus::Ok;
        }

        // ---- changing scale -----------------------------------------------------------------------------

        // Checks against 38 digits only. Going into a column or a cast target, use TryCast.
        [[nodiscard]] static constexpr DecimalStatus TryRescale(
            const Decimal& value,
            const Int fromScale,
            const Int toScale,
            const DecimalRoundingMode mode,
            Decimal* out
        ){
            if (toScale >= fromScale){
                const auto digits = toScale - fromScale;
                if (digits > MAX_DECIMAL_PRECISION){
                    if (!value.IsZero())
                        return DecimalStatus::Overflow;
                    *out = Decimal();
                    return DecimalStatus::Ok;
                }

                HugeInt scaled = 0;
                if (__builtin_mul_overflow(value.Mantissa(), static_cast<HugeInt>(DecimalDetail::POW10[digits]), &scaled))
                    return DecimalStatus::Overflow;

                *out = Decimal::FromMantissa(scaled);
                return out->Magnitude() > DecimalDetail::MAX_MAGNITUDE ? DecimalStatus::Overflow : DecimalStatus::Ok;
            }

            const auto digits = fromScale - toScale;
            if (digits > MAX_DECIMAL_PRECISION){
                *out = Decimal();
                return DecimalStatus::Ok;
            }

            const auto divisor = DecimalDetail::POW10[digits];
            const auto magnitude = value.Magnitude();
            auto quotient = magnitude / divisor;
            const auto remainder = magnitude % divisor;

            const auto half = divisor / 2;
            const bool roundUp = mode == DecimalRoundingMode::HalfUp
                ? remainder >= half
                : mode == DecimalRoundingMode::HalfEven && (remainder > half || (remainder == half && (quotient & 1) != 0));
            if (roundUp)
                quotient++;

            *out = Decimal::FromMagnitude(quotient, value.IsNegative());
            return DecimalStatus::Ok;
        }

        [[nodiscard]] static constexpr DecimalStatus TryCast(
            const Decimal& value,
            const Int fromScale,
            const DecimalType to,
            const DecimalRoundingMode mode,
            Decimal* out
        ){
            const auto status = Decimal::TryRescale(value, fromScale, to._scale, mode, out);
            if (status != DecimalStatus::Ok)
                return status;
            return out->FitsPrecision(to._precision) ? DecimalStatus::Ok : DecimalStatus::Overflow;
        }

        [[nodiscard]] static constexpr Decimal ScaleUpUnchecked(const Decimal& value, const Int digits){
            return Decimal::FromMantissa(value.Mantissa() * static_cast<HugeInt>(DecimalDetail::POW10[digits]));
        }

        template<IsInteger T>
        [[nodiscard]] static constexpr DecimalStatus TryFromInteger(const T value, const Int scale, Decimal* out){
            if (std::in_range<HugeInt>(value) == false)
                return DecimalStatus::Overflow;

            const auto mantissa = Decimal::FromMantissa(static_cast<HugeInt>(value));
            if (mantissa.Magnitude() > DecimalDetail::MAX_MAGNITUDE)
                return DecimalStatus::Overflow;
            return Decimal::TryRescale(mantissa, 0, scale, DecimalRoundingMode::Truncate, out);
        }

        template<IsInteger T>
        [[nodiscard]] static constexpr DecimalStatus TryFromInteger(const T value, const DecimalType type, Decimal* out){
            const auto status = Decimal::TryFromInteger(value, static_cast<Int>(type._scale), out);
            if (status != DecimalStatus::Ok)
                return status;
            return out->FitsPrecision(type._precision) ? DecimalStatus::Ok : DecimalStatus::Overflow;
        }

        template<IsInteger T>
        [[nodiscard]] constexpr DecimalStatus TryToInteger(const Int scale, const DecimalRoundingMode mode, T* out) const{
            Decimal whole;
            const auto status = Decimal::TryRescale(*this, scale, 0, mode, &whole);
            if (status != DecimalStatus::Ok)
                return status;
            if (!std::in_range<T>(whole.Mantissa()))
                return DecimalStatus::Overflow;
            *out = static_cast<T>(whole.Mantissa());
            return DecimalStatus::Ok;
        }

        // Accepts [+-]digits[.digits]. The result has the given scale: extra fraction digits are rounded.
        [[nodiscard]] static constexpr DecimalStatus TryParse(
            const StringView& text,
            const Int scale,
            const DecimalRoundingMode mode,
            Decimal* out
        ){
            const auto* data = text.Data();
            const auto size = static_cast<Int>(text.Size());
            Int position = 0;

            bool isNegative = false;
            if (position < size && (data[position] == '-' || data[position] == '+'))
                isNegative = data[position++] == '-';

            UnsignedHugeInt magnitude = 0;
            Int digitsSeen = 0;
            Int fractionDigits = 0;
            bool inFraction = false;
            Int firstDropped = -1;
            bool restIsNonZero = false;

            for (; position < size; position++){
                const auto c = data[position];
                if (c == '.'){
                    if (inFraction)
                        return DecimalStatus::InvalidFormat;
                    inFraction = true;
                    continue;
                }
                if (!DecimalDetail::IsDigit(c))
                    return DecimalStatus::InvalidFormat;

                digitsSeen++;
                const auto digit = static_cast<Int>(c - '0');

                if (inFraction && fractionDigits == scale){
                    if (firstDropped < 0)
                        firstDropped = digit;
                    else
                        restIsNonZero |= digit != 0;
                    continue;
                }

                if (magnitude > (DecimalDetail::MAX_MAGNITUDE - digit) / 10)
                    return DecimalStatus::Overflow;
                magnitude = magnitude * 10 + digit;
                fractionDigits += inFraction;
            }

            if (digitsSeen == 0)
                return DecimalStatus::InvalidFormat;

            for (; fractionDigits < scale; fractionDigits++){
                if (magnitude > DecimalDetail::MAX_MAGNITUDE / 10)
                    return DecimalStatus::Overflow;
                magnitude *= 10;
            }

            if (firstDropped >= 0 && DecimalDetail::ShouldRoundUp(mode, firstDropped, restIsNonZero, (magnitude & 1) != 0))
                magnitude++;
            if (magnitude > DecimalDetail::MAX_MAGNITUDE)
                return DecimalStatus::Overflow;

            *out = Decimal::FromMagnitude(magnitude, isNegative);
            return DecimalStatus::Ok;
        }

        [[nodiscard]] static constexpr DecimalStatus TryParse(
            const StringView& text,
            const DecimalType type,
            const DecimalRoundingMode mode,
            Decimal* out
        ){
            const auto status = Decimal::TryParse(text, static_cast<Int>(type._scale), mode, out);
            if (status != DecimalStatus::Ok)
                return status;
            return out->FitsPrecision(type._precision) ? DecimalStatus::Ok : DecimalStatus::Overflow;
        }

        // A SQL literal: the precision and scale come from how it is written, so "1.50" is DECIMAL(3,2)
        [[nodiscard]] static constexpr DecimalStatus TryParseLiteral(const StringView& text, DecimalType* type, Decimal* out){
            const auto* data = text.Data();
            const auto size = static_cast<Int>(text.Size());
            Int position = 0;
            if (position < size && (data[position] == '-' || data[position] == '+'))
                position++;

            Int scale = 0;
            Int significant = 0;
            bool inFraction = false;
            bool any = false;
            for (; position < size; position++){
                if (data[position] == '.'){
                    if (inFraction)
                        return DecimalStatus::InvalidFormat;
                    inFraction = true;
                    continue;
                }
                if (!DecimalDetail::IsDigit(data[position]))
                    return DecimalStatus::InvalidFormat;
                any = true;
                scale += inFraction;
                if (significant > 0 || data[position] != '0' || inFraction)
                    significant++;
            }

            if (!any)
                return DecimalStatus::InvalidFormat;
            if (significant > MAX_DECIMAL_PRECISION || scale > MAX_DECIMAL_PRECISION)
                return DecimalStatus::Overflow;

            const auto status = Decimal::TryParse(text, scale, DecimalRoundingMode::Truncate, out);
            if (status == DecimalStatus::Ok)
                *type = DecimalType(std::max({significant, scale, 1}), scale);
            return status;
        }

        [[nodiscard]] constexpr StringBuffer ToStringBuffer(const Int scale) const{
            char reversed[DECIMAL_STRING_SIZE]{};
            Int length = 0;
            auto magnitude = this->Magnitude();
            Int written = 0;

            while (magnitude != 0 || written <= scale){
                if (written == scale && scale > 0)
                    reversed[length++] = '.';

                reversed[length++] = static_cast<char>('0' + static_cast<Int>(magnitude % 10));
                magnitude /= 10;
                written++;
            }

            if (this->IsNegative())
                reversed[length++] = '-';

            char forward[DECIMAL_STRING_SIZE]{};
            for (Int i = 0; i < length; i++)
                forward[i] = reversed[length - 1 - i];

            return StringBuffer(forward, length);
        }

        [[nodiscard]] static constexpr Decimal Multiply(
            const Decimal& left, const Decimal& right, const Int droppedDigits, const DecimalRoundingMode mode
        ){
            Decimal result;
            Decimal::Check(Decimal::TryMultiply(left, right, droppedDigits, mode, &result));
            return result;
        }

        [[nodiscard]] static constexpr Decimal Divide(
            const Decimal& left, const Decimal& right, const Int scaleShift, const DecimalRoundingMode mode
        ){
            Decimal result;
            Decimal::Check(Decimal::TryDivide(left, right, scaleShift, mode, &result));
            return result;
        }

        [[nodiscard]] static constexpr Decimal Rescale(
            const Decimal& value, const Int fromScale, const Int toScale, const DecimalRoundingMode mode
        ){
            Decimal result;
            Decimal::Check(Decimal::TryRescale(value, fromScale, toScale, mode, &result));
            return result;
        }

        [[nodiscard]] static constexpr Decimal Cast(
            const Decimal& value, const Int fromScale, const DecimalType to, const DecimalRoundingMode mode
        ){
            Decimal result;
            Decimal::Check(Decimal::TryCast(value, fromScale, to, mode, &result));
            return result;
        }

        [[nodiscard]] static constexpr Decimal Parse(const StringView& text, const Int scale, const DecimalRoundingMode mode){
            Decimal result;
            Decimal::Check(Decimal::TryParse(text, scale, mode, &result));
            return result;
        }

        [[nodiscard]] static constexpr Decimal Parse(const StringView& text, const DecimalType type, const DecimalRoundingMode mode){
            Decimal result;
            Decimal::Check(Decimal::TryParse(text, type, mode, &result));
            return result;
        }

        [[nodiscard]] constexpr double ToDouble(const Int scale) const{
            return static_cast<double>(static_cast<long double>(this->Mantissa()) / static_cast<long double>(DecimalDetail::POW10[scale]));
        }

        [[nodiscard]] constexpr long double Interpolate(const Int scale) const{
            return static_cast<long double>(this->Mantissa()) / static_cast<long double>(DecimalDetail::POW10[scale]);
        }

        // Big-endian with the sign bit flipped: memcmp order == numeric order, for values of one scale
        constexpr void EncodeKey(object_t* out) const{
            auto bits = static_cast<UnsignedHugeInt>(this->Mantissa()) ^ (UnsignedHugeInt{1} << 127);
            for (Int i = DECIMAL_KEY_SIZE - 1; i >= 0; i--){
                out[i] = static_cast<object_t>(bits);
                bits >>= 8;
            }
        }

        [[nodiscard]] static constexpr Decimal DecodeKey(const object_t* data){
            UnsignedHugeInt bits = 0;
            for (Int i = 0; i < DECIMAL_KEY_SIZE; i++)
                bits = (bits << 8) | data[i];
            return Decimal::FromMantissa(static_cast<HugeInt>(bits ^ (UnsignedHugeInt{1} << 127)));
        }

        [[nodiscard]] constexpr std::size_t Hash() const{
            // Multiples of 10^scale have zero low bits. The multiply moves every bit upward and the shift
            // brings them back down, so round numbers still reach all the buckets of a power-of-two table.
            const auto bits = static_cast<UnsignedHugeInt>(this->Mantissa());
            const auto mixed = (static_cast<UnsignedBigInt>(bits) * 0x9E3779B97F4A7C15ULL)
                ^ (static_cast<UnsignedBigInt>(bits >> 64) * 0xC2B2AE3D27D4EB4FULL);
            return static_cast<std::size_t>(mixed ^ (mixed >> 32));
        }

        [[nodiscard]] String ToString(const ::Memory::IAllocator* allocator, Int scale) const;
        [[nodiscard]] StringValue ToStringValue(const ::Memory::IAllocator* allocator, Int scale) const;
    };

    static_assert(sizeof(Decimal) == sizeof(HugeInt) && alignof(Decimal) == 1);
    static_assert(std::is_trivially_copyable_v<Decimal> && std::is_trivially_destructible_v<Decimal>);
}

template<>
struct std::hash<DataTypes::Decimal> {
    constexpr size_t operator()(const DataTypes::Decimal& value) const noexcept {
        return value.Hash();
    }
};

template<>
struct std::numeric_limits<DataTypes::Decimal> {
    static constexpr bool is_specialized = true;

    static constexpr DataTypes::Decimal min() noexcept {
        static constexpr auto VALUE = DataTypes::Decimal::FromMantissa(Math::MinLimit<HugeInt>());
        return VALUE;
    }

    static constexpr DataTypes::Decimal max() noexcept {
        static constexpr auto VALUE = DataTypes::Decimal::FromMantissa(Math::MaxLimit<HugeInt>());
        return VALUE;
    }

    static constexpr int digits10 = 34;  // max base-10 precision
    static constexpr int radix = 10;     // base of your decimal
};

