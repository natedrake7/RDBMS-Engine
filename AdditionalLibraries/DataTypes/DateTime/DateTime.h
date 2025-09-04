#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <string>
using namespace std;

namespace DataTypes {
	constexpr uint32_t SECONDS_PER_MINUTE = 60;
	constexpr uint32_t SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
	constexpr uint32_t SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;
	constexpr uint32_t SECONDS_PER_WEEK = SECONDS_PER_DAY * 7;
	constexpr uint32_t SECONDS_PER_YEAR = SECONDS_PER_DAY * 365;

	class DateTime {
		int64_t timeStamp;

	protected:
		static void ValidateDate(int year, int month, int day, int hour, int minute, int second);

	public:
		DateTime();
		explicit DateTime(const int64_t& timestamp);
		~DateTime();

		[[nodiscard]] int GetYears() const;
		[[nodiscard]] unsigned int GetMonths() const;
		[[nodiscard]] unsigned int  GetDays() const;
		[[nodiscard]] long GetHours() const;
		[[nodiscard]] long GetMinutes() const;
		[[nodiscard]] long GetSeconds() const;
		[[nodiscard]] long GetMilliseconds() const;

		void AddSeconds(const int& seconds);
		void AddMinutes(const int& minutes);
		void AddHours(const int& hours);
		void AddDays(const int& days);
		void AddWeeks(const int& weeks);
		void AddMonths(const int& months);
		void AddYears(const int& years);

		static DateTime Now();
		static bool FromString(DateTime& outVal, const string& date, const string& format = "");
		static int DateTimeSize();

		[[nodiscard]] string ToString(const string& format = "%Y-%m-%d %H:%M:%S%OS") const;
		[[nodiscard]] const int64_t& GetUnixTimeStamp()const;

		static bool ValidateDate(const DateTime& datetime);

		friend ostream& operator<<(ostream& os, const DateTime& datetime);
	};
}


bool operator==(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator!=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator>=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator<=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator>(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator<(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);