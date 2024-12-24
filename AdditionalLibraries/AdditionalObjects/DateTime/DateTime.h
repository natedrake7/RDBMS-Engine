#pragma once

#include <cstdint>
#include <ctime>
#include <string>
using namespace std;

namespace DataTypes {
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

		[[nodiscard]] int GetYears() const;
		[[nodiscard]] int GetMonths() const;
		[[nodiscard]] int GetDays() const;
		[[nodiscard]] int GetHours() const;
		[[nodiscard]] int GetMinutes() const;
		[[nodiscard]] int GetSeconds() const;

		void AddSeconds(int seconds);
		void AddDays(int days);

		static DateTime Now();
		static time_t ToUnixTimeStamp(const string& date, const string& format = "%Y-%m-%d %H:%M:%S");
		static DateTime FromString(const string& date, const string& format = "%Y-%m-%d %H:%M:%S");
		static int DateTimeSize();

		[[nodiscard]] string ToString(const string& format = "%Y-%m-%d %H:%M:%S") const;
	};
}