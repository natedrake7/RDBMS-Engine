#pragma once

#include <cstdint>
#include <ctime>
#include <stdexcept>
#include <string>
#include <time.h>
#include <sstream>
#include <iomanip>
using namespace std;

constexpr uint32_t SECONDS_OF_DAY = 86400;


class DateTime {
	time_t timeStamp;

	protected:
		static void ValidateDate(int year, int month, int day, int hour, int minute, int second);
		static time_t ToUnixTimeStamp(int year, int month, int day, int hour, int minute, int second);

	public:
		DateTime();
		DateTime(int year, int month, int day, int hour = 0, int minute = 0, int second = 0);
		explicit DateTime(const time_t& timestamp);
		~DateTime();

		int GetYears() const;
		int GetMonths() const;
		int GetDays() const;
		int GetHours() const;
		int GetMinutes() const;
		int GetSeconds() const;

		void AddSeconds(int seconds);
		void AddDays(int days);

		static DateTime Now();
		static time_t ToUnixTimeStamp(const string& date, const string& format = "%Y-%m-%d %H:%M:%S");
		static DateTime FromString(const string& date, const string& format = "%Y-%m-%d %H:%M:%S");
		static int DateTimeSize();

		string ToString(const string& format = "%Y-%m-%d %H:%M:%S") const;

};