#pragma once

#include <chrono>
#include <string>

#include "StringView.h"
#include "../DataTypes/DataTypes.h"

namespace DataTypes {
	constexpr UnsignedInt SECONDS_PER_MINUTE = 60;
	constexpr UnsignedInt SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
	constexpr UnsignedInt SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;
	constexpr UnsignedInt SECONDS_PER_WEEK = SECONDS_PER_DAY * 7;
	constexpr UnsignedInt SECONDS_PER_YEAR = SECONDS_PER_DAY * 365;

	class DateTime {
		BigInt timeStamp;

		static inline const std::array<StringView, 15> DateTimeFormats = {
			// ISO 8601 with fractional seconds and timezone
			StringView("%Y-%m-%dT%H:%M:%S.%OS%z"),   // e.g., 2025-08-26T19:30:20.123+0200
			StringView("%Y-%m-%dT%H:%M:%S.%OSZ"),    // e.g., 2025-08-26T19:30:20.123Z
			StringView("%Y-%m-%dT%H:%M:%S.%OS"),     // e.g., 2025-08-26T19:30:20.123
			StringView("%Y-%m-%dT%H:%M:%SZ"),        // e.g., 2025-08-26T19:30:20Z
			StringView("%Y-%m-%dT%H:%M:%S"),         // e.g., 2025-08-26T19:30:20

			// Full date + time with fractional seconds
			StringView("%Y-%m-%d %H:%M:%S.%OS"),     // e.g., 2025-08-26 19:30:20.123

			// Full date + time (seconds precision)
			StringView("%Y-%m-%d %H:%M:%S"),         // e.g., 2025-08-26 19:30:20
			StringView("%d/%m/%Y %H:%M:%S"),         // e.g., 26/08/2025 19:30:20
			StringView("%m/%d/%Y %H:%M:%S"),         // e.g., 08/26/2025 19:30:20

			// Date only
			StringView("%Y-%m-%d"),                   // e.g., 2025-08-26
			StringView("%d/%m/%Y"),                   // e.g., 26/08/2025
			StringView("%m/%d/%Y"),                   // e.g., 08/26/2025
			StringView("%Y%m%d"),                     // e.g., 20250826

			// Time only
			StringView("%H:%M:%S"),                   // e.g., 19:30:20
			StringView("%H:%M")                       // e.g., 19:30
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
		static bool FromString(DateTime& outVal, const StringView& date, const StringView& format = "");
		static bool FromString(const StringView& str);
		static constexpr Int Size(){ return DATETIME_SIZE; }

		[[nodiscard]] String ToString(const ::Memory::IAllocator* allocator, const StringView& format = "%Y-%m-%d %H:%M:%S%OS") const;
		[[nodiscard]] BigInt UnixTimeStamp()const;

		static bool ValidateDate(const DateTime& datetime);

		// friend std::ostream& operator<<(std::ostream& os, const DateTime& datetime);
	    void Print(std::ostream& os, const ::Memory::IAllocator* allocator, const StringView& format = "%Y-%m-%d %H:%M:%S%OS") const;
	};
}


bool operator==(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator!=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator>=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator<=(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator>(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);
bool operator<(const DataTypes::DateTime& firstDate, const DataTypes::DateTime& secondDate);