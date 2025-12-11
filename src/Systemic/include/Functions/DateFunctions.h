#pragma once
#include "../DataTypes/DateTime.h"

#include <stdexcept>

namespace Functions::DateTime {
  enum class DateInterval {
    Year = 0,
    Quarter = 1,
    Month = 2,
    DayOfYear = 3,
    Day = 4,
    Week = 5,
    Weekday = 6,
    Hour = 7,
    Minute = 8,
    Second = 9,
    Millisecond = 10,
  };

  inline DataTypes::DateTime DateAdd(const DateInterval& interval, const int& number, const std::string& date) {

    DataTypes::DateTime result;
    DataTypes::DateTime::FromString(result, date);

    switch (interval) {
      case DateInterval::Year:
        result.AddYears(number);
        break;
      case DateInterval::Quarter:
        break;
      case DateInterval::Month:
        result.AddMonths(number);
        break;
      case DateInterval::DayOfYear:
        break;
      case DateInterval::Day:
        result.AddDays(number);
        break;
      case DateInterval::Week:
        result.AddWeeks(number);
        break;
      case DateInterval::Weekday:
        break;
      case DateInterval::Hour:
        result.AddHours(number);
        break;
      case DateInterval::Minute:
        result.AddMinutes(number);
        break;
      case DateInterval::Second:
        result.AddSeconds(number);
        break;
      case DateInterval::Millisecond:
        break;
      default:
        throw std::runtime_error("Invalid DateInterval");
    }

    return result;
  }


}