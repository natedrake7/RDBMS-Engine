#include "../../include/DataTypes/DateTime.h"
#include <iomanip>
#include <chrono>
#include <cmath>
#include <spanstream>

#include "Constants.h"
#include "DataTypes/String.h"

namespace DataTypes{
	DateTime::DateTime(){
		const auto timePoint = std::chrono::system_clock::now();
		this->timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch()).count();
	}

	DateTime::DateTime(const BigInt timestamp){
		this->timeStamp = timestamp;
	}

	DateTime::~DateTime() = default;

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

	void DateTime::AddWeeks(const Int weeks) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::days(7 * weeks);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddDays(const Int days) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::days(days);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddHours(const Int hours) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::hours(hours);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddMinutes(const Int minutes) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::minutes(minutes);

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

	bool DateTime::FromString(DateTime& outVal, const StringView& date, const StringView& format)
	{
		std::chrono::system_clock::time_point tp;

		if (!format.Empty()) {
			std::ispanstream ss(date);
			ss >> std::chrono::parse(format.Data(), tp);

			if (ss.fail())
				return false;
		}
		else {
			bool parsedDate = false;
			for (const auto& validFormat: DateTimeFormats) {
				std::ispanstream ss(date);
				ss >> std::chrono::parse(validFormat.Data(), tp);

				if (!ss.fail()) {
					parsedDate = true;
					break;
				}
			}

			if (!parsedDate)
				return false;
		}

		const auto millis = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();

		outVal = DateTime(millis);

		return true;
	}

	bool DateTime::FromString(const StringView& str)
	{
        bool parsedDate = false;
		for (const auto& validFormat: DateTimeFormats) {
            std::chrono::system_clock::time_point tp;
            std::ispanstream ss(str);

			ss >> std::chrono::parse(validFormat.Data(), tp);

			if (!ss.fail()) {
				parsedDate = true;
				break;
			}
		}

	    return parsedDate;
	}

    String DateTime::ToString(const ::Memory::IAllocator* allocator, const StringView& format) const
	{
	    const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));
	    const std::time_t t = std::chrono::system_clock::to_time_t(timePoint);
	    const auto* localTime = std::localtime(&t);

	    // Format date/time using strftime into a char buffer
	    char buffer[DATETIME_TO_STRING_BUFFER_SIZE] = {};
	    const auto bufferBytes = std::strftime(buffer, sizeof(buffer), format.Data(), localTime);

	    // Append milliseconds manually
	    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            timePoint.time_since_epoch()) % 1000;

	    char msBuffer[DATETIME_TO_STRING_MS_BUFFER_SIZE] = {};
	    const auto msBytes = std::snprintf(msBuffer, sizeof(msBuffer), ".%03lld", static_cast<long long>(ms.count()));

	    String result(allocator, static_cast<Int>(bufferBytes + msBytes));

	    result.Insert(0, buffer, static_cast<Int>(bufferBytes));
        result.Insert(static_cast<Int>(bufferBytes), msBuffer, msBytes);

	    return result;
	}


	BigInt DateTime::UnixTimeStamp() const { return this->timeStamp; }

	bool DateTime::ValidateDate(const DateTime &datetime){
		const auto& timestamp = datetime.UnixTimeStamp();

		return localtime(&timestamp) != nullptr;
	}

	void DateTime::ValidateDate(const int year, const int month, const int day, const int hour, const int minute, const int second)
	{
		if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour >= 24 || minute < 0 || minute >= 60 || second < 0 || second >= 60)
			throw std::invalid_argument("Invalid date/time components.");
	}

	// std::ostream & operator<<(std::ostream &os, const DateTime &datetime){
	// 	os << datetime.ToString();
	// 	return os;
	// }

}

bool operator!=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return !(firstDate == secondDate);
}

void DataTypes::DateTime::Print(
    std::ostream& os,
    const Memory::IAllocator* allocator,
    const StringView& format
) const{
    os << this->ToString(allocator, format);
}

bool operator==(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return firstDate.UnixTimeStamp() == secondDate.UnixTimeStamp();
}

bool operator>=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return !(firstDate < secondDate);
}
bool operator<=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return !(firstDate > secondDate);
}
bool operator>(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return firstDate.UnixTimeStamp() > secondDate.UnixTimeStamp();
}
bool operator<(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return firstDate.UnixTimeStamp() < secondDate.UnixTimeStamp();
}
