#include "DateTime.h"
#include <iomanip>
#include <chrono>

namespace DataTypes
{
	DateTime::DateTime()
	{
		this->timeStamp = time(nullptr);
	}

	DateTime::DateTime(const int year, const int month, const int day, const int hour, const int minute, const int second)
	{
		this->timeStamp = DateTime::ToUnixTimeStamp(year, month, day, hour, minute, second);
	}

	DateTime::DateTime(const time_t &timestamp)
	{
		this->timeStamp = timestamp;
	}

	DateTime::~DateTime() = default;

	int DateTime::GetYears() const
	{
		tm time = {};

#ifdef _WIN32
		localtime_s(&time, &this->timeStamp);
#else
		localtime_r(&this->timeStamp, &time);
#endif
		return time.tm_year + 1900;
	}

	int DateTime::GetMonths() const
	{
		tm time = {};

#ifdef _WIN32
		localtime_s(&time, &this->timeStamp);
#else
		localtime_r(&this->timeStamp, &time);
#endif
		// localtime_s(&time, &this->timeStamp);
		return time.tm_mon + 1;
	}

	int DateTime::GetDays() const
	{
		tm time = {};

#ifdef _WIN32
		localtime_s(&time, &this->timeStamp);
#else
		localtime_r(&this->timeStamp, &time);
#endif
		return time.tm_mday;
	}

	int DateTime::GetHours() const
	{
		tm time = {};

#ifdef _WIN32
		localtime_s(&time, &this->timeStamp);
#else
		localtime_r(&this->timeStamp, &time);
#endif
		return time.tm_hour;
	}

	int DateTime::GetMinutes() const
	{
		tm time = {};
		
#ifdef _WIN32
		localtime_s(&time, &this->timeStamp);
#else
		localtime_r(&this->timeStamp, &time);
#endif
		return time.tm_min;
	}

	int DateTime::GetSeconds() const
	{
		tm time = {};

#ifdef _WIN32
		localtime_s(&time, &this->timeStamp);
#else
		localtime_r(&this->timeStamp, &time);
#endif
		return time.tm_sec;
	}

	void DateTime::AddSeconds(const int seconds) { this->timeStamp += seconds; }

	void DateTime::AddDays(const int days) { this->timeStamp += days * SECONDS_OF_DAY; }

	DateTime DateTime::Now() { return {}; }

	int DateTime::DateTimeSize() { return sizeof(time_t); }

	time_t DateTime::ToUnixTimeStamp(const string &date, const string &format)
	{
		DateTime dateTime;

		auto result = DateTime::FromString(dateTime, date, format);

		return DateTime::ToUnixTimeStamp(dateTime.GetYears(), dateTime.GetMonths(), dateTime.GetDays(), dateTime.GetHours(), dateTime.GetMinutes(), dateTime.GetSeconds());
	}

	bool DateTime::FromString(DateTime& outVal, const string &date, const string &format)
	{
		tm time = {};
		
		istringstream ss(date);

		 ss >> get_time(&time, format.c_str());

		if (ss.fail())
			return false;

		outVal = {time.tm_year + 1900, time.tm_mon + 1, time.tm_mday, time.tm_hour, time.tm_min, time.tm_sec};

		return true;
	}

	string DateTime::ToString(const string &format) const
	{

		const auto timePoint = std::chrono::system_clock::from_time_t(this->timeStamp);

#ifdef _WIN32
		return std::format("{:%Y-%m-%d %H:%M:%S}", timePoint);
#else
		const std::time_t t = std::chrono::system_clock::to_time_t(timePoint);

		// Convert to std::tm (local time)
		const struct tm* localTime = std::localtime(&t);

		// Create a buffer to hold the formatted time
		char buffer[100];

		std::strftime(buffer, sizeof(buffer), format.c_str(), localTime);

		return {buffer};
#endif
	}

	const time_t & DateTime::GetUnixTimeStamp() const { return this->timeStamp; }

	bool DateTime::ValidateDate(const DateTime &datetime){
		const auto& timestamp = datetime.GetUnixTimeStamp();

		return localtime(&timestamp) != nullptr;
	}

	time_t DateTime::ToUnixTimeStamp(const int year, const int month, const int day, const int hour, const int minute, const int second)
	{
		tm time = {};

		time.tm_year = year - 1900;
		time.tm_mon = month - 1;
		time.tm_mday = day;
		time.tm_hour = hour;
		time.tm_min = minute;
		time.tm_sec = second;

		return mktime(&time);
	}

	void DateTime::ValidateDate(const int year, const int month, const int day, const int hour, const int minute, const int second)
	{
		if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour >= 24 || minute < 0 || minute >= 60 || second < 0 || second >= 60)
			throw invalid_argument("Invalid date/time components.");
	}

	ostream & operator<<(ostream &os, const DateTime &datetime){
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
