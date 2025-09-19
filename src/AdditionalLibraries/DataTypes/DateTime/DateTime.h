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

		static inline const std::array<std::string, 15> DateTimeFormats = {
			// ISO 8601 with fractional seconds and timezone
			"%Y-%m-%dT%H:%M:%S.%OS%z",   // e.g., 2025-08-26T19:30:20.123+0200
			"%Y-%m-%dT%H:%M:%S.%OSZ",    // e.g., 2025-08-26T19:30:20.123Z
			"%Y-%m-%dT%H:%M:%S.%OS",     // e.g., 2025-08-26T19:30:20.123
			"%Y-%m-%dT%H:%M:%SZ",        // e.g., 2025-08-26T19:30:20Z
			"%Y-%m-%dT%H:%M:%S",         // e.g., 2025-08-26T19:30:20

			// Full date + time with fractional seconds
			"%Y-%m-%d %H:%M:%S.%OS",     // e.g., 2025-08-26 19:30:20.123

			// Full date + time (seconds precision)
			"%Y-%m-%d %H:%M:%S",         // e.g., 2025-08-26 19:30:20
			"%d/%m/%Y %H:%M:%S",         // e.g., 26/08/2025 19:30:20
			"%m/%d/%Y %H:%M:%S",         // e.g., 08/26/2025 19:30:20

			// Date only
			"%Y-%m-%d",                   // e.g., 2025-08-26
			"%d/%m/%Y",                   // e.g., 26/08/2025
			"%m/%d/%Y",                   // e.g., 08/26/2025
			"%Y%m%d",                     // e.g., 20250826

			// Time only
			"%H:%M:%S",                   // e.g., 19:30:20
			"%H:%M"                       // e.g., 19:30
		    };

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
		static bool FromString(const string& date);
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