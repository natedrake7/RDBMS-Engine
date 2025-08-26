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
		time_t timeStamp;
	protected:
		static void ValidateDate(int year, int month, int day, int hour, int minute, int second);
		static time_t ToUnixTimeStamp(const int& year, const int& month, const int& day, const int& hour, const int& minute, const int& second);

	public:
		DateTime();
		DateTime(const int& year, const int& month, const int& day, const int& hour = 0, const int& minute = 0, const int& second = 0);
		explicit DateTime(const time_t& timestamp);
		~DateTime();

		[[nodiscard]] int GetYears() const;
		[[nodiscard]] int GetMonths() const;
		[[nodiscard]] int GetDays() const;
		[[nodiscard]] int GetHours() const;
		[[nodiscard]] int GetMinutes() const;
		[[nodiscard]] int GetSeconds() const;

		void AddSeconds(const int& seconds);
		void AddMinutes(const int& minutes);
		void AddHours(const int& hours);
		void AddDays(const int& days);
		void AddWeeks(const int& weeks);
		void AddMonths(const int& months);
		void AddYears(const int& years);

		static DateTime Now();
		static time_t ToUnixTimeStamp(const string& date, const string& format = "%Y-%m-%d %H:%M:%S");
		static bool FromString(DateTime& outVal, const string& date, const string& format = "%Y-%m-%d %H:%M:%S");
		static int DateTimeSize();

		[[nodiscard]] string ToString(const string& format = "%Y-%m-%d %H:%M:%S") const;
		[[nodiscard]] const time_t& GetUnixTimeStamp()const;

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