#include "DateTime.h"

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
	
	localtime_s(&time, &this->timeStamp);
	return time.tm_year + 1900; 
}

int DateTime::GetMonths() const 
{ 
	tm time = {};

	localtime_s(&time, &this->timeStamp);
	return time.tm_mon + 1;
}

int DateTime::GetDays() const 
{ 
	tm time = {};

	localtime_s(&time, &this->timeStamp); 
	return time.tm_mday;
}

int DateTime::GetHours() const 
{ 
	tm time = {};
	localtime_s(&time, &this->timeStamp);

	return time.tm_hour; 
}

int DateTime::GetMinutes() const 
{ 
	tm time = {};
	localtime_s(&time, &this->timeStamp);
	return time.tm_min; 
}

int DateTime::GetSeconds() const 
{ 
	tm time = {};

	localtime_s(&time, &this->timeStamp);
	return time.tm_sec;
}

void DateTime::AddSeconds(const int seconds) { this->timeStamp += seconds; }

void DateTime::AddDays(const int days) { this->timeStamp += days * SECONDS_OF_DAY; }

DateTime DateTime::Now() { return { }; }

int DateTime::DateTimeSize() { return sizeof(time_t); }

time_t DateTime::ToUnixTimeStamp(const string &date, const string &format)
{
	const DateTime dateTime = DateTime::FromString(date, format);

	return DateTime::ToUnixTimeStamp(dateTime.GetYears(), dateTime.GetMonths(), dateTime.GetDays(), dateTime.GetHours(), dateTime.GetMinutes(), dateTime.GetSeconds());
}

DateTime DateTime::FromString(const string& date, const string& format)
{
	tm time = {};
	istringstream ss(date);

	ss >> get_time(&time, format.c_str());

	return{ time.tm_year + 1900, time.tm_mon + 1, time.tm_mday, time.tm_hour, time.tm_min, time.tm_sec };
}

string DateTime::ToString(const string& format) const
{
	tm time {};
	localtime_s(&time, &this->timeStamp);

	char buffer[100];
	strftime(buffer, sizeof(buffer), format.c_str(), &time);

	return { buffer };
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
	if (year < 1970 
		|| month < 1 || month > 12 
		|| day < 1 || day > 31 
		||hour < 0 || hour >= 24
		|| minute < 0 || minute >= 60 
		|| second < 0 || second >= 60)
		throw invalid_argument("Invalid date/time components.");
}