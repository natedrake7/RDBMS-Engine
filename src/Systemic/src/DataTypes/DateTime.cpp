#include "../../include/DataTypes/DateTime.h"

#include <charconv>
#include <chrono>
#include "DataTypes/String.h"
#include "DataTypes/StringValue.h"

namespace DataTypes{

    bool MatchFormat(const StringView& format, const StringView& input, ParsedFields& out) {
        const auto* formatStr = format.Data();
        const auto* formatStrEnd = formatStr + format.Size();

        const auto* inputStr = input.Data();
        const auto* inputStrEnd = inputStr + input.Size();

        while (formatStr < formatStrEnd) {
            if (*formatStr != '%') {
                if (inputStr >= inputStrEnd || *inputStr != *formatStr)
                    return false;

                inputStr++;
                formatStr++;
                continue;
            }

            formatStr++;
            if (formatStr >= formatStrEnd)
                return false;

            switch (*formatStr) {
            case 'Y': if (!ReadDigits(inputStr, inputStrEnd, 4, out.year)) return false; break;
            case 'm': if (!ReadDigits(inputStr, inputStrEnd, 2, out.month)) return false; break;
            case 'd': if (!ReadDigits(inputStr, inputStrEnd, 2, out.day)) return false; break;
            case 'H': if (!ReadDigits(inputStr, inputStrEnd, 2, out.hour)) return false; break;
            case 'M': if (!ReadDigits(inputStr, inputStrEnd, 2, out.minute)) return false; break;
            case 'S': if (!ReadDigits(inputStr, inputStrEnd, 2, out.second)) return false; break;
            case 'z':
                if (!ReadTzOffset(inputStr, inputStrEnd, out.tzOffsetMinutes))
                    return false;
                break;
            case 'O':
                ++formatStr;
                if (formatStr >= formatStrEnd || *formatStr != 'S') return false;
                if (!ReadFractionalMillis(inputStr, inputStrEnd, out.millis)) return false;
                break;
            default: return false;
            }
            ++formatStr;
        }

        return inputStr == inputStrEnd;
    }

    bool ReadDigits(const char*& p, const char* end, const int width, int& out){
	    if (end - p < width || *p == '-' || *p == '+') return false;

	    const auto res = std::from_chars(p, p + width, out);
	    if (res.ec != std::errc() || res.ptr != p + width)
	        return false;

	    p += width;
	    return true;
    }

    bool ReadFractionalMillis(const char*& p, const char* end, int& out){
        if (p >= end || *p < '0' || *p > '9') return false;

        const char* runEnd = p;
        while (runEnd < end && *runEnd >= '0' && *runEnd <= '9') ++runEnd;

        const int digits = static_cast<int>(runEnd - p);
        const int usedDigits = digits < 3 ? digits : 3;

        int value = 0;
        std::from_chars(p, p + usedDigits, value);
        for (int i = digits; i < 3; ++i) value *= 10;

        p = runEnd;
        out = value;
        return true;
    }

    bool ReadTzOffset(const char*& p, const char* end, int& out){
        if (end - p < 5)
            return false;

        const char sign = *p;
        if (sign != '+' && sign != '-')
            return false;

        const char* cursor = p + 1;
        int hours, minutes;
        if (!ReadDigits(cursor, end, 2, hours))
            return false;
        if (!ReadDigits(cursor, end, 2, minutes))
            return false;

        out = (sign == '-' ? -1 : 1) * (hours * 60 + minutes);
        p = cursor;
        return true;
    }

	void DateTime::ValidateDate(const int year, const int month, const int day, const int hour, const int minute, const int second){
        if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour >= 24 || minute < 0 || minute >= 60 || second < 0 || second >= 60)
            throw std::invalid_argument("Invalid date/time components.");
    }

	DateTime::DateTime(){
        const auto timePoint = std::chrono::system_clock::now();
        this->timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch()).count();
    }

	DateTime::DateTime(const BigInt timestamp){
        this->timeStamp = timestamp;
    }

	int DateTime::GetYears() const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        // Convert to sys_days (floor to days)
        const auto dp = floor<std::chrono::days>(timePoint);

        // Convert to calendar year_month_day
        const std::chrono::year_month_day ymd{dp};

        return static_cast<int>(ymd.year());
    }

	unsigned int DateTime::GetMonths() const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        // Convert to sys_days (floor to days)
        const auto dp = floor<std::chrono::days>(timePoint);

        // Convert to calendar year_month_day
        const std::chrono::year_month_day ymd{dp};

        return static_cast<unsigned int>(ymd.month());
    }

	unsigned int  DateTime::GetDays() const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        // Convert to sys_days (floor to days)
        const auto dp = floor<std::chrono::days>(timePoint);

        // Convert to calendar year_month_day
        const std::chrono::year_month_day ymd{dp};

        return static_cast<unsigned int>(ymd.day());
    }

	BigInt DateTime::GetHours() const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        // Convert to sys_days (floor to days)
        const auto dp = floor<std::chrono::days>(timePoint);

        const auto time_since_midnight = timePoint - dp; // duration since midnight

        return duration_cast<std::chrono::hours>(time_since_midnight).count();
    }

	BigInt DateTime::GetMinutes() const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        // Convert to sys_days (floor to days)
        const auto dp = floor<std::chrono::days>(timePoint);

        const auto time_since_midnight = timePoint - dp; // duration since midnight

        const auto hours = duration_cast<std::chrono::hours>(time_since_midnight);

        return duration_cast<std::chrono::minutes>(time_since_midnight - hours).count();
    }

	BigInt DateTime::GetSeconds() const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        // Convert to sys_days (floor to days)
        const auto dp = floor<std::chrono::days>(timePoint);

        const auto time_since_midnight = timePoint - dp; // duration since midnight

        const auto minutes = duration_cast<std::chrono::minutes>(time_since_midnight);

        return duration_cast<std::chrono::seconds>(time_since_midnight - minutes).count();
    }

	BigInt DateTime::GetMilliseconds() const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        // Convert to sys_days (floor to days)
        const auto dp = floor<std::chrono::days>(timePoint);

        const auto time_since_midnight = timePoint - dp; // duration since midnight

        const auto seconds = duration_cast<std::chrono::seconds>(time_since_midnight);

        return duration_cast<std::chrono::milliseconds>(time_since_midnight - seconds).count();
    }

	void DateTime::AddSeconds(const Int seconds) {
        auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        tp += std::chrono::seconds(seconds);

        this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

	void DateTime::AddMinutes(const Int minutes) {
        auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        tp += std::chrono::minutes(minutes);

        this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

	void DateTime::AddHours(const Int hours) {
        auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        tp += std::chrono::hours(hours);

        this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }


	void DateTime::AddDays(const Int days) {
        auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        tp += std::chrono::days(days);

        this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

	void DateTime::AddWeeks(const Int weeks) {
        auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        tp += std::chrono::days(7 * weeks);

        this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

	void DateTime::AddMonths(const Int months){

        auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

        const auto dp = floor<std::chrono::days>(tp);               // floor to whole days

        std::chrono::year_month_day ymd{dp};

        ymd += std::chrono::months{months};                   // add months

        tp = std::chrono::sys_days(ymd) + (tp - dp);

        this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

	void DateTime::AddYears(const Int years) {
        auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));
        const auto dp = floor<std::chrono::days>(tp);

        std::chrono::year_month_day ymd{dp};
        ymd += std::chrono::years{years};                            // add years

        tp = std::chrono::sys_days(ymd) + (tp - dp);          // keep the time-of-day

        this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

	DateTime DateTime::Now() { return DateTime(); }

    bool DateTime::FromStringView(DateTime& outVal, const StringView& date, const StringView& format){
        ParsedFields fields;

        if (!format.Empty()) {
            if (!MatchFormat(format, date, fields))
                return false;
        }
        else {
            bool parsedDate = false;
            for (const auto& validFormat: DATETIME_FORMATS) {
                fields.Reset();
                if (MatchFormat(validFormat, date, fields)) {
                    parsedDate = true;
                    break;
                }
            }

            if (!parsedDate)
                return false;
        }

        const std::chrono::year_month_day ymd{
            std::chrono::year(fields.year),
            std::chrono::month(fields.month),
            std::chrono::day(fields.day)
        };

        if (!ymd.ok())
            return false;

        auto tp = std::chrono::sys_days(ymd)
            + std::chrono::hours(fields.hour)
            + std::chrono::minutes(fields.minute)
            + std::chrono::seconds(fields.second)
            + std::chrono::milliseconds(fields.millis);

        if (fields.HasTz())
            tp -= std::chrono::minutes(fields.tzOffsetMinutes);

        const auto millis = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();

        outVal = DateTime(millis);

        return true;
    }

    bool DateTime::FromStringView(const StringView& str){
        DateTime discard;
        return FromStringView(discard, str, StringView());
    }

	DateTime::StringBuffer DateTime::ToStringBuffer(const StringView& format) const{
        const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));
        const auto t = std::chrono::system_clock::to_time_t(timePoint);
        const auto* localTime = std::localtime(&t);

        // Format date/time using strftime into a char buffer
        StringBuffer buffer;
        const auto bufferBytes = std::strftime(
            buffer.Data(),
            StringBuffer::Capacity(),
            format.Data(),
            localTime
        );
        buffer.SetSize(static_cast<Int>(bufferBytes));

        // Append milliseconds manually
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            timePoint.time_since_epoch()) % 1000;

        const auto msBytes = std::snprintf(buffer.Data() + bufferBytes, StringBuffer::Capacity() - buffer.Size(), ".%03lld", static_cast<long long>(ms.count()));
        buffer.SetSize(static_cast<Int>(bufferBytes) + msBytes);

        return buffer;
    }

	String DateTime::ToString(const ::Memory::IAllocator* allocator, const StringView& format) const{
        auto buffer = this->ToStringBuffer(format);
        return String(buffer.Data(), buffer.Size(), allocator);
    }

    StringValue DateTime::ToStringValue(const ::Memory::IAllocator* allocator, const StringView& format) const{
        const auto buffer = this->ToStringBuffer(format);
        return StringValue::Create(allocator, buffer.Data(), buffer.Size());
    }

    BigInt DateTime::UnixTimeStamp() const { return this->timeStamp; }

	bool DateTime::ValidateDate() const{
        return std::localtime(&this->timeStamp) != nullptr;
    }

    void DateTime::Print(std::ostream& os, const StringView& format) const{
        const auto buffer = this->ToStringBuffer(format);
        os.write(buffer.Data(), buffer.Size());
    }

    bool operator==(const DateTime& lhs, const DateTime& rhs) { return lhs.timeStamp == rhs.timeStamp;}
    bool operator!=(const DateTime& lhs, const DateTime& rhs) { return lhs.timeStamp != rhs.timeStamp; }

    bool operator>=(const DateTime& lhs, const DateTime& rhs) { return !(lhs < rhs); }
    bool operator<=(const DateTime& lhs, const DateTime& rhs) { return !(lhs > rhs); }

    bool operator>(const DateTime& lhs, const DateTime& rhs) { return lhs.timeStamp > rhs.timeStamp; }
    bool operator<(const DateTime& lhs, const DateTime& rhs) { return lhs.timeStamp < rhs.timeStamp;}


}

