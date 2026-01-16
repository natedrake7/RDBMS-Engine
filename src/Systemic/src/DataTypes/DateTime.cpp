#include "../../include/DataTypes/DateTime.h"
#include <iomanip>
#include <chrono>
#include <cmath>

namespace DataTypes{
	DateTime::DateTime(){
		const auto timePoint = std::chrono::system_clock::now();
		this->timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch()).count();
	}

	DateTime::DateTime(const int64_t &timestamp){
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

	long DateTime::GetHours() const{
		const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		// Convert to sys_days (floor to days)
		const auto dp = floor<std::chrono::days>(timePoint);

		const auto time_since_midnight = timePoint - dp; // duration since midnight

		return duration_cast<std::chrono::hours>(time_since_midnight).count();
	}

	long DateTime::GetMinutes() const{
		const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		// Convert to sys_days (floor to days)
		const auto dp = floor<std::chrono::days>(timePoint);

		const auto time_since_midnight = timePoint - dp; // duration since midnight

		const auto hours = duration_cast<std::chrono::hours>(time_since_midnight);

		return duration_cast<std::chrono::minutes>(time_since_midnight - hours).count();
	}

	long DateTime::GetSeconds() const{
		const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		// Convert to sys_days (floor to days)
		const auto dp = floor<std::chrono::days>(timePoint);

		const auto time_since_midnight = timePoint - dp; // duration since midnight

		const auto minutes = duration_cast<std::chrono::minutes>(time_since_midnight);

		return duration_cast<std::chrono::seconds>(time_since_midnight - minutes).count();
	}

	long DateTime::GetMilliseconds() const{
		const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		// Convert to sys_days (floor to days)
		const auto dp = floor<std::chrono::days>(timePoint);

		const auto time_since_midnight = timePoint - dp; // duration since midnight

		const auto seconds = duration_cast<std::chrono::seconds>(time_since_midnight);

		return duration_cast<std::chrono::milliseconds>(time_since_midnight - seconds).count();
	}

	void DateTime::AddSeconds(const int& seconds) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::seconds(seconds);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddWeeks(const int& weeks) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::days(7 * weeks);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddDays(const int& days) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::days(days);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddHours(const int& hours) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::hours(hours);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddMinutes(const int& minutes) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		tp += std::chrono::minutes(minutes);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}


	void DateTime::AddMonths(const int &months){

		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

		const auto dp = floor<std::chrono::days>(tp);               // floor to whole days

		std::chrono::year_month_day ymd{dp};

		ymd += std::chrono::months{months};                   // add months

		tp = std::chrono::sys_days(ymd) + (tp - dp);

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	void DateTime::AddYears(const int& years) {
		auto tp = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));
		const auto dp = floor<std::chrono::days>(tp);

		std::chrono::year_month_day ymd{dp};
		ymd += std::chrono::years{years};                            // add years

		tp = std::chrono::sys_days(ymd) + (tp - dp);          // keep the time-of-day

		this->timeStamp = duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
	}

	DateTime DateTime::Now() { return {}; }

	bool DateTime::FromString(DateTime& outVal, const std::string &date, const std::string &format)
	{
		std::chrono::system_clock::time_point tp;

		if (!format.empty()) {
			std::istringstream ss(date);

			ss >> std::chrono::parse(format, tp);

			if (ss.fail())
				return false;
		}
		else {
			bool parsedDate = false;
			for (const auto& validFormat: DateTimeFormats) {
				std::istringstream ss(date);

				ss >> std::chrono::parse(validFormat, tp);

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

	bool DateTime::FromString(const std::string &date)
	{
		std::chrono::system_clock::time_point tp;

		bool parsedDate = false;
		for (const auto& validFormat: DateTimeFormats) {
			std::istringstream ss(date);

			ss >> std::chrono::parse(validFormat, tp);

			if (!ss.fail()) {
				parsedDate = true;
				break;
			}
		}

		if (!parsedDate)
			return false;

		return true;
	}

	std::string DateTime::ToString(const std::string &format) const
	{
		const auto timePoint = std::chrono::system_clock::time_point(std::chrono::milliseconds(this->timeStamp));

#ifdef _WIN32
		return std::format("{:%Y-%m-%d %H:%M:%S%OS}", timePoint);
#else
		const std::time_t t = std::chrono::system_clock::to_time_t(timePoint);

		// Convert to std::tm (local time)
		const auto* localTime = std::localtime(&t);

		std::ostringstream oss;
		oss << std::put_time(localTime, format.c_str());

		// Append milliseconds
		const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch()) % 1000;
		oss << '.' << std::setw(3) << std::setfill('0') << ms.count();

		return oss.str();
#endif
	}

	const int64_t & DateTime::GetUnixTimeStamp() const { return this->timeStamp; }

	bool DateTime::ValidateDate(const DateTime &datetime){
		const auto& timestamp = datetime.GetUnixTimeStamp();

		return localtime(&timestamp) != nullptr;
	}

	void DateTime::ValidateDate(const int year, const int month, const int day, const int hour, const int minute, const int second)
	{
		if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour >= 24 || minute < 0 || minute >= 60 || second < 0 || second >= 60)
			throw std::invalid_argument("Invalid date/time components.");
	}

	std::ostream & operator<<(std::ostream &os, const DateTime &datetime){
		os << datetime.ToString();
		return os;
	}

}

bool operator!=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return !(firstDate == secondDate);
}

bool operator==(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return firstDate.GetUnixTimeStamp() == secondDate.GetUnixTimeStamp();
}

bool operator>=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return !(firstDate < secondDate);
}
bool operator<=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return !(firstDate > secondDate);
}
bool operator>(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return firstDate.GetUnixTimeStamp() > secondDate.GetUnixTimeStamp();
}
bool operator<(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate) {
	return firstDate.GetUnixTimeStamp() < secondDate.GetUnixTimeStamp();
}
