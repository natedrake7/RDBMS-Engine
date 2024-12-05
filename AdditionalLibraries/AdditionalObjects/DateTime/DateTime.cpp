#include "DateTime.h"

DateTime::DateTime()
{
	this->timeStamp = time(nullptr);
}

DateTime::DateTime(int year, int month, int day, int hour, int minute, int second)
{
	this->timeStamp = this->ToUnixTimeStamp(year, month, day, hour, minute, second);
}

DateTime::~DateTime() = default;

int DateTime::GetYears() const { return localtime(&this->timeStamp)->tm_year + 1900; }

int DateTime::GetMonths() const { return localtime(&this->timeStamp)->tm_mon + 1; }

int DateTime::GetDays() const { return localtime(&this->timeStamp)->tm_mday; }

int DateTime::GetHours() const { return localtime(&this->timeStamp)->tm_hour; }

int DateTime::GetMinutes() const { return localtime(&this->timeStamp)->tm_min; }

int DateTime::GetSeconds() const { return localtime(&this->timeStamp)->tm_sec; }

void DateTime::AddSeconds(int seconds) { this->timeStamp += seconds; }

void DateTime::AddDays(int days) { this->timeStamp += days * SECONDS_OF_DAY; }

DateTime DateTime::Now() { return DateTime(); }

DateTime DateTime::FromString(const string& date, const string& format)
{
	struct tm time = {};
	istringstream ss(date);

	ss >> get_time(&time, format.c_str());

	return DateTime(time.tm_year + 1900, time.tm_mon + 1, time.tm_mday, time.tm_hour, time.tm_min, time.tm_sec);
}

string DateTime::ToString(const string& format) const
{
	struct tm* time = localtime(&this->timeStamp);

	char buffer[100];
	strftime(buffer, sizeof(buffer), format.c_str(), time);

	return string(buffer);
}

time_t DateTime::ToUnixTimeStamp(int year, int month, int day, int hour, int minute, int second)
{
	struct tm time = {};

	time.tm_year = year - 1900;
	time.tm_mon = month - 1;
	time.tm_mday = day;
	time.tm_hour = hour;
	time.tm_min = minute;
	time.tm_sec = second;

	return mktime(&time);
}

void DateTime::ValidateDate(int year, int month, int day, int hour, int minute, int second) 
{
	if (year < 1970 
		|| month < 1 || month > 12 
		|| day < 1 || day > 31 
		||hour < 0 || hour >= 24
		|| minute < 0 || minute >= 60 
		|| second < 0 || second >= 60)
		throw invalid_argument("Invalid date/time components.");
}