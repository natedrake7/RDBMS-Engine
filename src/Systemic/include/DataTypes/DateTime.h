#pragma once

#include <chrono>
#include <string>

#include "String.h"
#include "../DataTypes/DataTypes.h"

namespace DataTypes {
	constexpr UnsignedInt SECONDS_PER_MINUTE = 60;
	constexpr UnsignedInt SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
	constexpr UnsignedInt SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;
	constexpr UnsignedInt SECONDS_PER_WEEK = SECONDS_PER_DAY * 7;
	constexpr UnsignedInt SECONDS_PER_YEAR = SECONDS_PER_DAY * 365;

	class DateTime {
		BigInt timeStamp;

		static inline const std::array<String, 15> DateTimeFormats = {
			// ISO 8601 with fractional seconds and timezone
			String("%Y-%m-%dT%H:%M:%S.%OS%z"),   // e.g., 2025-08-26T19:30:20.123+0200
			String("%Y-%m-%dT%H:%M:%S.%OSZ"),    // e.g., 2025-08-26T19:30:20.123Z
			String("%Y-%m-%dT%H:%M:%S.%OS"),     // e.g., 2025-08-26T19:30:20.123
			String("%Y-%m-%dT%H:%M:%SZ"),        // e.g., 2025-08-26T19:30:20Z
			String("%Y-%m-%dT%H:%M:%S"),         // e.g., 2025-08-26T19:30:20

			// Full date + time with fractional seconds
			String("%Y-%m-%d %H:%M:%S.%OS"),     // e.g., 2025-08-26 19:30:20.123

			// Full date + time (seconds precision)
			String("%Y-%m-%d %H:%M:%S"),         // e.g., 2025-08-26 19:30:20
			String("%d/%m/%Y %H:%M:%S"),         // e.g., 26/08/2025 19:30:20
			String("%m/%d/%Y %H:%M:%S"),         // e.g., 08/26/2025 19:30:20

			// Date only
			String("%Y-%m-%d"),                   // e.g., 2025-08-26
			String("%d/%m/%Y"),                   // e.g., 26/08/2025
			String("%m/%d/%Y"),                   // e.g., 08/26/2025
			String("%Y%m%d"),                     // e.g., 20250826

			// Time only
			String("%H:%M:%S"),                   // e.g., 19:30:20
			String("%H:%M")                       // e.g., 19:30
		};

	protected:
		static void ValidateDate(Int year, Int month, Int day, Int hour, Int minute, Int second);

	public:
		DateTime();
		explicit DateTime(BigInt timestamp);

        DateTime(const DateTime& other) = default;
        DateTime& operator=(const DateTime& other) = default;
	    DateTime(DateTime&& other) = default;
	    DateTime& operator=(DateTime&& other) = default;

	    ~DateTime();

		[[nodiscard]] Int GetYears() const;
		[[nodiscard]] UnsignedInt GetMonths() const;
		[[nodiscard]] UnsignedInt  GetDays() const;
		[[nodiscard]] BigInt GetHours() const;
		[[nodiscard]] BigInt GetMinutes() const;
		[[nodiscard]] BigInt GetSeconds() const;
		[[nodiscard]] BigInt GetMilliseconds() const;

		void AddSeconds(Int seconds);
		void AddMinutes(Int minutes);
		void AddHours(Int hours);
		void AddDays(Int days);
		void AddWeeks(Int weeks);
		void AddMonths(Int months);
		void AddYears(Int years);

		static DateTime Now();
		static bool FromString(DateTime& outVal, const std::string& date, const std::string& format = "");
		static bool FromString(const String& str);
		inline static constexpr Int Size(){ return DATETIME_SIZE; }

		[[nodiscard]] String ToString(const String& format = "%Y-%m-%d %H:%M:%S%OS") const;
		[[nodiscard]] BigInt GetUnixTimeStamp()const;

		static bool ValidateDate(const DateTime& datetime);

		friend std::ostream& operator<<(std::ostream& os, const DateTime& datetime);
	};
}


bool operator==(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator!=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator>=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator<=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator>(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator<(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);